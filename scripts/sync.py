"""Pinned HF -> bounded local cache -> internal OSS, with persistent retries."""
import concurrent.futures as cf
from contextlib import closing
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import json
import logging
import multiprocessing
import os
from pathlib import Path, PurePosixPath
import random
import re
import shutil
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import oss2
import requests
from tasklib import validate_settings, validate_manifest, task_identity

if not __debug__:
    raise RuntimeError('Python -O disables integrity assertions and is unsupported')


class IntegrityError(Exception):
    pass


class DestinationConflict(Exception):
    pass


class UploadFailure(Exception):
    def __init__(self,kind,status,headers):
        super().__init__(kind,status,headers)
        self.kind=kind; self.status=status; self.response=SimpleNamespace(headers=headers)


def initialize_uploader(root):
    global upload_sync
    upload_sync=Sync(root)


def upload_file(row,name,crc):
    try: upload_sync.upload(row,name,crc)
    except Exception as e:
        status,headers=http_failure(e)
        # SDK exceptions may not be pickleable; retain retry/auth/integrity semantics without secrets.
        headers={k:v for k,v in headers.items() if k.lower() in ['retry-after','ratelimit']}
        raise UploadFailure(type(e).__name__,status,headers) from None


class TargetBucket(oss2.Bucket):
    def complete_multipart_upload(self, key, upload_id, parts, headers=None, **kwargs):
        # oss2's resume helper filters this header out before completing multipart uploads.
        headers=oss2.CaseInsensitiveDict(headers or {})
        headers['x-oss-forbid-overwrite']='true'
        return super().complete_multipart_upload(key,upload_id,parts,headers=headers,**kwargs)


def log(event, **fields):
    print(json.dumps(dict(time=time.time(),event=event,**fields),ensure_ascii=False),flush=True)


def fingerprint(stream, size, sha256=None, git_blob_id=None):
    h=hashlib.sha256() if sha256 else hashlib.sha1(b'blob '+str(size).encode()+b'\0')
    crc=oss2.utils.Crc64(); n=0
    for data in iter(lambda:stream.read(8*1024**2),b''):
        n+=len(data); h.update(data); crc.update(data)
    if n!=size or h.hexdigest()!=(sha256 or git_blob_id):
        raise IntegrityError('source digest or length mismatch')
    return str(crc.crc)


def fingerprint_file(name, size, sha256, git_blob_id):
    # Native crcmod holds the GIL; isolate full-file scans from network threads.
    with open(name,'rb') as src:
        return fingerprint(src,size,sha256,git_blob_id)


def retry_delay(attempts):
    return min(1800,5*2**min(attempts,9))+random.uniform(0,5)


def http_failure(error):
    seen=set()
    while error is not None and id(error) not in seen:
        seen.add(id(error)); response=getattr(error,'response',None)
        code=getattr(error,'status',None) or getattr(response,'status_code',None)
        if code: return code,getattr(response,'headers',{})
        error=error.__cause__ or error.__context__
    return None,{}


def rate_limit_delay(headers):
    headers={k.lower():str(v) for k,v in headers.items()}
    waits=[int(v) for v in re.findall(r'\bt=(\d+)',headers.get('ratelimit',''))]
    value=headers.get('retry-after','')
    if value.isdigit(): waits.append(int(value))
    elif value:
        try: waits.append(parsedate_to_datetime(value).timestamp()-time.time())
        except (ValueError,TypeError,OverflowError): pass
    return max(1,max(waits) if waits else 300)+2


class Sync:
    def __init__(self, root):
        self.root=Path(root); self.cfg=json.loads((self.root/'settings.json').read_text())
        self.cfg.setdefault('hf_requests_per_second',10)
        validate_settings(self.cfg)
        self.secret={k:os.environ[v] for k,v in {'hf_token':'HF_TOKEN','ak':'OSS_ACCESS_KEY_ID','sk':'OSS_ACCESS_KEY_SECRET'}.items()}
        assert self.cfg['mode'] in ['http','xet'] and 1<=self.cfg['workers']<=64
        self.cache=self.root/'cache'; self.cache.mkdir(exist_ok=True)
        self.durable=Path(self.cfg['durable']); self.durable.mkdir(parents=True,exist_ok=True,mode=0o700)
        (self.durable/'oss-checkpoints').mkdir(exist_ok=True)
        self.db=self.root/'state.sqlite3'
        if not self.db.exists() and (self.durable/'state.sqlite3').exists():
            shutil.copy2(self.durable/'state.sqlite3',self.db)
        assert 1<=self.cfg['upload_workers']<=64
        assert 1<=self.cfg.get('upload_part_threads',2)<=8
        self.thread=threading.local()
        self.auth_blocked=threading.Event()
        self.rate_limit_until=0
        os.environ.update(HF_ENDPOINT='https://huggingface.co',HF_TOKEN=self.secret['hf_token'],
          HTTPS_PROXY=self.cfg['proxy'],HTTP_PROXY=self.cfg['proxy'],
          HF_HOME=str(self.root/'hf-home'),HF_XET_CACHE=str(self.root/'xet-cache'),
          HF_XET_HIGH_PERFORMANCE='1',HF_HUB_DISABLE_TELEMETRY='1',HF_HUB_DISABLE_PROGRESS_BARS='1',
          HF_HUB_DOWNLOAD_TIMEOUT='60',HF_HUB_ETAG_TIMEOUT='30',
          HF_HUB_DISABLE_XET='1' if self.cfg['mode']=='http' else '0')
        for key in ('ALL_PROXY','all_proxy'): os.environ.pop(key,None)
        logging.disable(logging.CRITICAL)
        from huggingface_hub import hf_hub_download
        import huggingface_hub.file_download as download
        # HF 0.30 ignores HF_HUB_DISABLE_XET; select its official HTTP fallback explicitly.
        if self.cfg['mode']=='http': download.is_xet_available=lambda:False
        self.transport_lock=threading.Lock()
        proxies=self.cfg.get('download_proxies',[self.cfg['proxy']])
        assert len(proxies)==1 or self.cfg['mode']=='http'
        self.proxy_load=dict.fromkeys(proxies,0)
        self.transport={'http_calls':0,'xet_calls':0,'resumed_http_calls':0,'resumed_bytes':0}
        for method,metric in [('http_get','http_calls'),('xet_get','xet_calls')]:
            original=getattr(download,method)
            def tracked(*args,_original=original,_metric=metric,**kwargs):
                with self.transport_lock:
                    self.transport[_metric]+=1
                    if _metric=='http_calls' and kwargs.get('resume_size',0)>0:
                        self.transport['resumed_http_calls']+=1
                        self.transport['resumed_bytes']+=kwargs['resume_size']
                return _original(*args,**kwargs)
            setattr(download,method,tracked)
        self.hf_download=hf_hub_download
        self.hf_request_lock=threading.Lock(); self.hf_next_request=0; self.hf_request_count=0
        rate=self.cfg.get('hf_requests_per_second',0)
        assert rate==0 or 0<rate<=16
        if rate: self.configure_hf_pacing()

    def wait_hf_request(self):
        with self.hf_request_lock:
            while True:
                if self.auth_blocked.is_set(): raise RuntimeError('authentication blocked')
                delay=max(self.hf_next_request-time.monotonic(),self.rate_limit_until-time.time())
                if delay<=0: break
                time.sleep(min(delay,1))
            self.hf_next_request=time.monotonic()+1/self.cfg['hf_requests_per_second']
            self.hf_request_count+=1

    def configure_hf_pacing(self):
        from huggingface_hub import configure_http_backend
        from huggingface_hub.utils._http import UniqueRequestIdAdapter
        owner=self
        class PacedAdapter(UniqueRequestIdAdapter):
            def send(self,request,*args,**kwargs):
                is_hub=urlsplit(request.url).hostname=='huggingface.co'
                if is_hub: owner.wait_hf_request()
                response=super().send(request,*args,**kwargs)
                if is_hub and response.status_code==429:
                    # Stop already-queued requests immediately; failed() persists the cooldown.
                    with owner.transport_lock:
                        owner.rate_limit_until=max(owner.rate_limit_until,time.time()+rate_limit_delay(response.headers))
                return response
        def factory():
            session=requests.Session()
            session.trust_env=False
            session.mount('https://',PacedAdapter());session.mount('http://',PacedAdapter())
            return session
        configure_http_backend(factory)

    def connect(self):
        c=sqlite3.connect(self.db,timeout=60); c.row_factory=sqlite3.Row
        c.execute('PRAGMA busy_timeout=60000'); c.execute('PRAGMA synchronous=NORMAL')
        return c

    def initialize(self):
        m=json.loads((self.root/'manifest.json').read_text())
        validate_manifest(m)
        assert m['repo']==self.cfg['repo'] and m['revision']==self.cfg['revision']
        assert len(m['objects'])==m['files'] and sum(f['bytes'] for f in m['objects'])==m['bytes']
        with closing(self.connect()) as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.executescript('''CREATE TABLE IF NOT EXISTS files(
              path TEXT PRIMARY KEY,size INTEGER NOT NULL,sha256 TEXT,git_blob_id TEXT,
              status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
              next_retry REAL NOT NULL DEFAULT 0,reserved INTEGER NOT NULL DEFAULT 0,
              cached_file TEXT,cache_mtime INTEGER,crc64 TEXT,error TEXT,verified_at REAL,
              updated REAL NOT NULL DEFAULT 0);
              CREATE INDEX IF NOT EXISTS work ON files(status,next_retry);
              CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT);''')
            old=c.execute("SELECT value FROM metadata WHERE key='revision'").fetchone()
            assert old is None or old[0]==m['revision'],'State belongs to another snapshot'
            identity=task_identity(self.cfg,m)
            saved=c.execute("SELECT value FROM metadata WHERE key='task_identity'").fetchone()
            if old is not None and (saved is None or saved[0]!=identity):
                raise ValueError('Task manifest or destination changed; use a new task directory')
            if old is None: c.execute("INSERT INTO metadata VALUES('task_identity',?)",(identity,))
            if old is None:
                rows=[]
                for f in m['objects']:
                    p=PurePosixPath(f['path'])
                    assert not p.is_absolute() and '..' not in p.parts and '\0' not in f['path']
                    assert f['bytes']>=0 and (f['sha256'] or f['git_blob_id'])
                    assert f['bytes']+65536<=self.cfg['stage_limit_bytes']
                    rows.append((f['path'],f['bytes'],f['sha256'],f['git_blob_id']))
                c.executemany('INSERT INTO files(path,size,sha256,git_blob_id) VALUES(?,?,?,?)',rows)
                c.execute("INSERT INTO metadata VALUES('revision',?)",(m['revision'],))
                c.execute("INSERT INTO metadata VALUES('total_bytes',?)",(str(m['bytes']),))
            c.execute("UPDATE files SET status='retry',next_retry=0,error='interrupted; resume cached data' WHERE status IN ('downloading','checking','ready','uploading')")
            limit=c.execute("SELECT value FROM metadata WHERE key='hf_rate_limit_until'").fetchone()
            if limit: self.rate_limit_until=float(limit[0])
            c.commit()
            assert c.execute('SELECT count(*),sum(size) FROM files').fetchone()[:]==(m['files'],m['bytes'])
        manifest_backup=self.durable/'manifest.json'
        if not manifest_backup.exists():
            shutil.copy2(self.root/'manifest.json',manifest_backup)
        self.backup()

    def update(self,path,**fields):
        fields['updated']=time.time()
        with closing(self.connect()) as c:
            c.execute('UPDATE files SET '+','.join(k+'=?' for k in fields)+' WHERE path=?',(*fields.values(),path))
            c.commit()

    def bucket(self):
        if not hasattr(self.thread,'bucket'):
            session=oss2.Session(pool_size=8); session.session.trust_env=False
            self.thread.bucket=TargetBucket(oss2.Auth(self.secret['ak'],self.secret['sk']),
                self.cfg['endpoint'],self.cfg['bucket'],session=session,connect_timeout=30,enable_crc=True)
        return self.thread.bucket

    def cache_dir(self,path):
        return self.cache/hashlib.sha256(path.encode()).hexdigest()

    def cleanup(self,path):
        p=self.cache_dir(path)
        assert p.parent==self.cache and len(p.name)==64
        if p.exists(): shutil.rmtree(p)

    def existing(self,b,row,key):
        try: head=b.head_object(key)
        except oss2.exceptions.NoSuchKey: return False
        if head.content_length!=row['size']: raise DestinationConflict('existing size differs')
        expected=row['sha256'] or row['git_blob_id']
        meta=head.headers
        crc=meta.get('x-oss-hash-crc64ecma')
        if meta.get('x-oss-meta-hf-digest')==expected and crc is not None and meta.get('x-oss-meta-source-crc64')==crc:
            return True
        obj=b.get_object(key)
        try: actual=fingerprint(obj,row['size'],row['sha256'],row['git_blob_id'])
        except IntegrityError as e: raise DestinationConflict('existing object digest differs') from e
        finally: obj.close()
        if crc is None or actual!=str(crc): raise DestinationConflict('existing object CRC differs')
        return True

    def download(self,row):
        path=row['path']; key=self.cfg['prefix']+path
        b=self.bucket()
        if self.existing(b,row,key):
            self.cleanup(path)
            self.update(path,status='verified',reserved=0,error=None,verified_at=time.time())
            log('verified',path=path,bytes=row['size'],reused=True); return None
        cached=Path(row['cached_file']) if row['cached_file'] else None
        usable=(cached and cached.is_file() and cached.resolve().is_relative_to(self.cache_dir(path).resolve())
                and cached.stat().st_size==row['size'] and cached.stat().st_mtime_ns==row['cache_mtime'] and row['crc64'])
        if usable: return str(cached),row['crc64']
        with self.transport_lock:
            proxy=min(self.proxy_load,key=self.proxy_load.get); self.proxy_load[proxy]+=row['size']
        try:
            kwargs=dict(repo_type='dataset',revision=self.cfg['revision'],token=self.secret['hf_token'],cache_dir=self.cache_dir(path))
            try:
                name=Path(self.hf_download(self.cfg['repo'],path,proxies={'http':proxy,'https':proxy},**kwargs))
            except (OSError,requests.RequestException) as e:
                # Keep the shared proxy's failover on network errors; never retry auth/HTTP or disk errors here.
                if proxy==self.cfg['proxy'] or http_failure(e)[0] is not None or getattr(e,'errno',None) is not None: raise
                log('download_proxy_fallback',error_type=type(e).__name__)
                name=Path(self.hf_download(self.cfg['repo'],path,
                    proxies={'http':self.cfg['proxy'],'https':self.cfg['proxy']},**kwargs))
        finally:
            with self.transport_lock: self.proxy_load[proxy]-=row['size']
        assert name.resolve().is_relative_to(self.cache_dir(path).resolve())
        self.update(path,status='checking')
        return str(name),None

    def upload(self,row,name,crc):
        path=row['path']; key=self.cfg['prefix']+path; b=self.bucket()
        self.update(path,status='uploading')
        headers={'x-oss-forbid-overwrite':'true','x-oss-meta-hf-digest':row['sha256'] or row['git_blob_id'],
                 'x-oss-meta-hf-revision':self.cfg['revision'],'x-oss-meta-source-crc64':str(crc)}
        oss2.resumable_upload(b,key,name,headers=headers,
            store=oss2.ResumableStore(root=str(self.durable),dir='oss-checkpoints'),
            multipart_threshold=64*1024**2,part_size=32*1024**2,num_threads=self.cfg.get('upload_part_threads',2))
        head=b.head_object(key)
        if head.content_length!=row['size'] or str(head.headers.get('x-oss-hash-crc64ecma'))!=str(crc):
            raise IntegrityError('OSS CRC or length mismatch')
        self.cleanup(path)
        self.update(path,status='verified',reserved=0,error=None,verified_at=time.time())
        log('verified',path=path,bytes=row['size'],reused=False)

    def failed(self,row,e):
        path=row['path']; code,headers=http_failure(e)
        kind=e.kind if isinstance(e,UploadFailure) else type(e).__name__
        if code==429:
            with self.transport_lock:
                self.rate_limit_until=max(self.rate_limit_until,time.time()+rate_limit_delay(headers))
                with closing(self.connect()) as c:
                    c.execute("INSERT OR REPLACE INTO metadata VALUES('hf_rate_limit_until',?)",(str(self.rate_limit_until),)); c.commit()
            log('rate_limited',retry_at=self.rate_limit_until)
        err=json.dumps(dict(type=kind,http_status=code))
        if isinstance(e,IntegrityError) or (isinstance(e,UploadFailure) and kind=='IntegrityError'):
            self.cleanup(path)
            self.update(path,cached_file=None,cache_mtime=None,crc64=None)
        blocked=isinstance(e,DestinationConflict) or (isinstance(e,UploadFailure) and kind=='DestinationConflict') or code in [401,403] or row['attempts']>=getattr(self,'cfg',{}).get('max_attempts',12)
        self.update(path,status='blocked' if blocked else 'retry',error=err,
                    next_retry=max(time.time()+retry_delay(row['attempts']),self.rate_limit_until if code==429 else 0))
        log('blocked' if blocked else 'retry',path=path,error_type=kind,http_status=code)
        if code in [401,403]: self.auth_blocked.set()

    def admit(self,slots):
        if time.time()<self.rate_limit_until: return []
        admitted=[]
        with closing(self.connect()) as c:
            reserved=c.execute('SELECT coalesce(sum(size+65536),0) FROM files WHERE reserved=1').fetchone()[0]
            size_order='DESC' if self.cfg.get('resource_largest_first',False) else 'ASC'
            rows=c.execute(f"""SELECT * FROM files WHERE status IN ('pending','retry') AND next_retry<=?
                ORDER BY reserved DESC,size {size_order},path LIMIT 2048""",(time.time(),)).fetchall()
            free=shutil.disk_usage(self.cache).free
            for row in rows:
                cost=row['size']+65536
                if not row['reserved'] and (reserved+cost>self.cfg['stage_limit_bytes'] or free-cost<self.cfg['disk_reserve_bytes']): continue
                if not row['reserved']: reserved+=cost; free-=cost
                c.execute("UPDATE files SET status='downloading',reserved=1,attempts=attempts+1,updated=? WHERE path=?",(time.time(),row['path']))
                r=dict(row); r['attempts']+=1; admitted.append(r)
                if len(admitted)==slots: break
            c.commit()
        return admitted

    def summary(self):
        with closing(self.connect()) as c:
            rows=c.execute('SELECT status,count(*),coalesce(sum(size),0) FROM files GROUP BY status').fetchall()
            s={r[0]:dict(files=r[1],bytes=r[2]) for r in rows}
            total=sum(x['bytes'] for x in s.values()); verified=s.get('verified',dict(files=0,bytes=0))
            blocked=[dict(r) for r in c.execute("SELECT path,error FROM files WHERE status='blocked' LIMIT 8")]
        return dict(states=s,total_bytes=total,verified_bytes=verified['bytes'],verified_files=verified['files'],
                    percent=verified['bytes']/total*100 if total else 0,blocked_samples=blocked,
                    hf_cooldown_seconds=max(0,self.rate_limit_until-time.time()))

    def backup(self):
        tmp=self.durable/'state.backup.tmp'
        with closing(self.connect()) as src,closing(sqlite3.connect(tmp)) as dest: src.backup(dest)
        os.replace(tmp,self.durable/'state.sqlite3')

    def run(self):
        self.initialize(); running={}; ready=[]; upload_turn=0
        last_report=time.monotonic()-30; last_backup=time.monotonic()
        previous=self.summary()['verified_bytes']; previous_time=time.monotonic()
        log('started',workers=self.cfg['workers'],upload_workers=self.cfg['upload_workers'],checksum_workers=2,
            mode=self.cfg['mode'],endpoint=self.cfg['endpoint'],oss_proxy=False)
        with cf.ThreadPoolExecutor(max_workers=self.cfg['workers'],thread_name_prefix='download') as downloads, \
             cf.ProcessPoolExecutor(max_workers=2,mp_context=multiprocessing.get_context('spawn')) as checks, \
             cf.ProcessPoolExecutor(max_workers=max(1,self.cfg['upload_workers']-1),mp_context=multiprocessing.get_context('spawn'),
                                    initializer=initialize_uploader,initargs=(self.root,)) as bulk_uploads, \
             cf.ThreadPoolExecutor(max_workers=self.cfg['upload_workers'],thread_name_prefix='upload') as uploads:
            while True:
                for f,(stage,row,name) in list(running.items()):
                    if f.done():
                        del running[f]
                        try:
                            result=f.result()
                            if stage=='upload' or (stage=='download' and result is None): continue
                            if stage=='download':
                                name,crc=result
                                if crc is None:
                                    future=checks.submit(fingerprint_file,name,row['size'],row['sha256'],row['git_blob_id'])
                                    running[future]=('check',row,name); continue
                            else: crc=result
                            self.update(row['path'],status='ready',cached_file=name,cache_mtime=Path(name).stat().st_mtime_ns,crc64=crc)
                            ready.append((row,name,crc))
                        except Exception as e: self.failed(row,e)
                if self.auth_blocked.is_set():
                    log('authentication_blocked'); break
                upload_slots=self.cfg['upload_workers']-sum(v[0]=='upload' for v in running.values())
                for _ in range(min(upload_slots,len(ready))):
                    # Keep one slot available for small files while bulk transfers are busy.
                    bulk=sum(v[0]=='upload' and v[1]['size']>=64*1024**2 for v in running.values())
                    eligible=[i for i,v in enumerate(ready) if v[0]['size']<64*1024**2 or bulk<max(1,self.cfg['upload_workers']-1)]
                    if not eligible: break
                    # Drain small files to free admission slots; every fourth dispatch is FIFO for fairness.
                    index=min(eligible,key=lambda i:ready[i][0]['size']) if upload_turn%4 else eligible[0]
                    row,name,crc=ready.pop(index); upload_turn+=1
                    future=(bulk_uploads.submit(upload_file,row,name,crc) if self.cfg.get('upload_processes',False) and row['size']>=64*1024**2
                            else uploads.submit(self.upload,row,name,crc))
                    running[future]=('upload',row,name)
                # Bound queued small files as well as reserved bytes across all stages.
                slots=min(self.cfg['workers']-sum(v[0]=='download' for v in running.values()),
                          self.cfg['workers']*2-len(running)-len(ready))
                if slots>0:
                    for row in self.admit(slots): running[downloads.submit(self.download,row)]=('download',row,None)
                now=time.monotonic()
                if now-last_report>=30:
                    s=self.summary(); delta=s['verified_bytes']-previous
                    s.update(MBs=delta/max(now-previous_time,0.001)/1e6,mode=self.cfg['mode'],workers=self.cfg['workers'],
                             upload_workers=self.cfg['upload_workers'],upload_part_threads=self.cfg.get('upload_part_threads',2),upload_processes=self.cfg.get('upload_processes',False),checksum_workers=2,transport=dict(self.transport),
                             download_proxy_load=dict(self.proxy_load),hf_requests_per_second=self.cfg.get('hf_requests_per_second',0),hf_request_count=getattr(self,'hf_request_count',0))
                    log('progress',**s)
                    tmp=self.root/'status.tmp'; tmp.write_text(json.dumps(dict(time=time.time(),**s),indent=2)+'\n')
                    os.replace(tmp,self.root/'status.json')
                    previous=s['verified_bytes']; previous_time=now; last_report=now
                    if not running and not ready and set(s['states'])=={'verified'}:
                        self.backup(); log('complete',**s); return
                    if not running and not ready and set(s['states'])<={'verified','blocked'}:
                        self.backup(); raise RuntimeError('Blocked files require review; task is incomplete')
                if now-last_backup>=300:
                    try: self.backup(); last_backup=now
                    except Exception as e: log('checkpoint_backup_error',error_type=type(e).__name__); last_backup=now
                time.sleep(0.5)
        self.backup()
        raise RuntimeError('Authentication blocked; task is incomplete')


if __name__=='__main__':
    if len(sys.argv)!=2: raise SystemExit('Usage: python scripts/sync.py TASK_DIRECTORY')
    root=Path(sys.argv[1]).resolve()
    root.mkdir(exist_ok=True)
    with (root/'service.lock').open('w') as service_lock:
        fcntl.flock(service_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        Sync(root).run()

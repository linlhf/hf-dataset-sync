import hashlib
import os
import concurrent.futures as cf
import io
import pickle
import multiprocessing
import json
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import oss2
from sync import Sync, TargetBucket, fingerprint, fingerprint_file, IntegrityError, DestinationConflict, UploadFailure, upload_file, retry_delay, http_failure, rate_limit_delay


def process(s,row):
    try:
        result=s.download(row)
        if result is None: return
        name,crc=result
        if crc is None: crc=fingerprint_file(name,row['size'],row['sha256'],row['git_blob_id'])
        s.update(row['path'],status='ready',cached_file=name,cache_mtime=Path(name).stat().st_mtime_ns,crc64=crc)
        s.upload(row,name,crc)
    except Exception as e: s.failed(row,e)


def fake_process_upload(row,name,crc):
    if row['path']=='bad': raise IntegrityError('test digest mismatch')
    Path(name).write_text(crc)


def initialize_test_uploader():
    import sync
    sync.upload_sync=SimpleNamespace(upload=fake_process_upload)


class SyncTest(unittest.TestCase):
    def setUp(self):
        env=patch.dict(os.environ,{'HF_TOKEN':'test','OSS_ACCESS_KEY_ID':'test','OSS_ACCESS_KEY_SECRET':'test'})
        env.start();self.addCleanup(env.stop)

    def test_hf_pacing_counts_requests_and_respects_cooldown(self):
        s=Sync.__new__(Sync);s.cfg={'hf_requests_per_second':4};s.hf_request_lock=threading.Lock()
        s.hf_next_request=0;s.hf_request_count=0;s.rate_limit_until=0;s.auth_blocked=threading.Event()
        clock=[100.0]
        with patch('sync.time.monotonic',side_effect=lambda:clock[0]),patch('sync.time.time',side_effect=lambda:clock[0]), \
             patch('sync.time.sleep',side_effect=lambda dt:clock.__setitem__(0,clock[0]+dt)):
            for _ in range(3):s.wait_hf_request()
            self.assertEqual(clock[0],100.5);self.assertEqual(s.hf_request_count,3)
            s.rate_limit_until=103;s.wait_hf_request();self.assertEqual(clock[0],103)

    def test_hf_adapter_pauses_on_429_but_not_cdn_or_oss(self):
        import httpx
        from huggingface_hub import set_client_factory,get_session
        s=Sync.__new__(Sync);s.transport_lock=threading.Lock();s.rate_limit_until=0;calls=[]
        s.cfg={'proxy':''};s.proxy_load={'':0};s.thread=threading.local()
        s.wait_hf_request=lambda:calls.append('request')
        s.configure_hf_pacing()
        try:
            with patch.object(httpx.HTTPTransport,'handle_request',side_effect=lambda req:httpx.Response(429,headers={'Retry-After':'5'})),patch('sync.time.time',return_value=100):
                for url in ['https://huggingface.co/a','https://us.aws.cdn.hf.co/b','https://example-bucket.oss-cn-wulanchabu-internal.aliyuncs.com/c']:
                    get_session().get(url)
            self.assertEqual(len(calls),1);self.assertEqual(s.rate_limit_until,107)
        finally:set_client_factory(lambda:httpx.Client())

    def test_httpx_routes_each_thread_without_environment_proxy(self):
        import httpx
        from huggingface_hub import set_client_factory,get_session
        s=Sync.__new__(Sync);s.cfg={'proxy':''};s.proxy_load={'http://127.0.0.1:1082':0}
        s.thread=threading.local();s.wait_hf_request=lambda:None
        calls=[]
        def route(*,proxy,trust_env):
            self.assertFalse(trust_env)
            def request(req):
                calls.append(proxy);return httpx.Response(200)
            return httpx.MockTransport(request)
        with patch('sync.httpx.HTTPTransport',side_effect=route):
            s.configure_hf_pacing()
            try:
                client=get_session();client.get('https://huggingface.co/file')
                s.thread.proxy='http://127.0.0.1:1082'
                client.get('https://cas-bridge.xethub.hf.co/file')
                self.assertEqual(calls,[None,s.thread.proxy])
            finally:set_client_factory(lambda:httpx.Client())

    def test_largest_resource_first_keeps_small_files_in_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);cfg=dict(endpoint='https://oss-cn-wulanchabu-internal.aliyuncs.com',bucket='example-bucket',prefix='datasets/example/',
                mode='http',workers=32,upload_workers=6,durable=str(root/'durable'),proxy='http://127.0.0.1:1080',repo='example/dataset',
                revision='a'*40,stage_limit_bytes=1000000,disk_reserve_bytes=1,resource_largest_first=True)
            (root/'settings.json').write_text(json.dumps(cfg))
            objects=[dict(path='asset/'+str(n),bytes=n,sha256='b'*64,git_blob_id=None) for n in [1,100,10]]
            (root/'manifest.json').write_text(json.dumps(dict(repo=cfg['repo'],revision=cfg['revision'],files=3,bytes=111,objects=objects)))
            s=Sync(root);s.initialize();rows=s.admit(2)
            self.assertEqual([r['size'] for r in rows],[100,10])
            rows+=s.admit(1);self.assertEqual([r['size'] for r in rows],[100,10,1])
            self.assertEqual(s.summary()['total_bytes'],111)
            s.cfg['prefix']='different/'
            with self.assertRaises(ValueError):s.initialize()

    def test_retry_budget_retains_failed_file(self):
        s=Sync.__new__(Sync);s.cfg={'max_attempts':2};s.auth_blocked=threading.Event();s.rate_limit_until=0
        updates=[];s.update=lambda *args,**kw:updates.append(kw)
        s.failed({'path':'asset','attempts':2},OSError('transient'))
        self.assertEqual(updates[-1]['status'],'blocked')
        self.assertFalse(s.auth_blocked.is_set())

    def test_spawn_upload_and_error_transport(self):
        with tempfile.TemporaryDirectory() as tmp, cf.ProcessPoolExecutor(max_workers=1,
                mp_context=multiprocessing.get_context('spawn'),initializer=initialize_test_uploader) as pool:
            path=Path(tmp)/'result'
            self.assertIsNone(pool.submit(upload_file,dict(path='ok'),str(path),'crc').result(timeout=10))
            self.assertEqual(path.read_text(),'crc')
            with self.assertRaises(UploadFailure) as caught:
                pool.submit(upload_file,dict(path='bad'),str(path),'crc').result(timeout=10)
            self.assertEqual(caught.exception.kind,'IntegrityError')

    def test_upload_process_errors_keep_retry_and_integrity_semantics(self):
        for error in [IntegrityError('bad'),DestinationConflict('exists'),OSError('network')]:
            with patch('sync.upload_sync',SimpleNamespace(upload=lambda *args:(_ for _ in ()).throw(error)),create=True):
                with self.assertRaises(UploadFailure) as caught: upload_file({},'file','crc')
            e=pickle.loads(pickle.dumps(caught.exception))
            s=Sync.__new__(Sync);s.auth_blocked=threading.Event();s.rate_limit_until=0
            updates=[];cleaned=[];s.update=lambda *args,**kw:updates.append(kw);s.cleanup=lambda p:cleaned.append(p)
            s.failed(dict(path='file',attempts=1),e)
            self.assertEqual(updates[-1]['status'],'blocked' if isinstance(error,DestinationConflict) else 'retry')
            self.assertEqual(bool(cleaned),isinstance(error,IntegrityError))
        e=pickle.loads(pickle.dumps(UploadFailure('ServerError',403,{})))
        s.failed(dict(path='file',attempts=1),e)
        self.assertEqual(updates[-1]['status'],'blocked');self.assertTrue(s.auth_blocked.is_set())
        self.assertEqual(http_failure(UploadFailure('ServerError',429,{'Retry-After':'5'})),(429,{'Retry-After':'5'}))

    def test_bulk_uploads_leave_a_slot_for_small_files(self):
        held=[]; order=[]
        class Pool:
            def __init__(self,*args,**kwargs): pass
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def submit(self,fn,*args):
                f=cf.Future()
                if fn==s.upload and args[0]['size']>=64*1024**2 and not order:
                    held.append((f,fn,args));return f
                f.set_result(fn(*args));return f
        with tempfile.TemporaryDirectory() as tmp:
            name=Path(tmp)/'blob';name.write_bytes(b'x')
            s=Sync.__new__(Sync);s.root=Path(tmp)
            s.cfg=dict(workers=8,upload_workers=2,mode='http',endpoint='test')
            s.transport={};s.proxy_load={};s.auth_blocked=threading.Event()
            waiting=[dict(path=str(i),size=n) for i,n in enumerate([100*1024**2,200*1024**2])];done=[];errors=[]
            s.initialize=lambda:None;s.backup=lambda:None;s.update=lambda *args,**kwargs:None
            def admit(slots):
                rows=waiting[:slots];del waiting[:slots]
                if not rows and held and not order: rows=[dict(path='small',size=1)]
                return rows
            def upload(row,*args):
                order.append(row['path']);done.append(row['size'])
                if row['path']=='small':
                    for f,fn,a in held: f.set_result(fn(*a))
            s.admit=admit;s.download=lambda row:(str(name),'crc');s.upload=upload;s.failed=lambda row,e:errors.append(e)
            s.summary=lambda:dict(verified_bytes=sum(done),states={'verified':{}} if len(done)==3 else {'pending':{}})
            with patch('sync.cf.ThreadPoolExecutor',Pool),patch('sync.cf.ProcessPoolExecutor',Pool), \
                 patch('sync.time.sleep'),patch('sync.time.monotonic',side_effect=range(0,10000,31)):
                s.run()
            self.assertEqual(order,['small','0','1']);self.assertEqual(errors,[])

    def test_download_byte_routing_releases_load_and_falls_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=Sync.__new__(Sync);s.cache=Path(tmp);s.transport_lock=threading.Lock()
            s.cfg=dict(proxy='http://127.0.0.1:1080',repo='test',revision='a'*40,prefix='public/');s.secret={'hf_token':'test'}
            s.proxy_load={'http://127.0.0.1:1082':100,'http://127.0.0.1:1083':0}
            s.bucket=lambda:None;s.existing=lambda *args:False;s.update=lambda *args,**kwargs:None
            row=dict(path='file',size=3,cached_file=None);calls=[]
            def download(*args,**kwargs):
                calls.append(kwargs['proxies']['https'])
                if len(calls)==1: raise OSError('short network response')
                name=Path(kwargs['cache_dir'])/'blob';name.parent.mkdir();name.write_bytes(b'abc');return str(name)
            s.hf_download=download
            self.assertEqual(s.download(row)[1],None)
            self.assertEqual(calls,['http://127.0.0.1:1083',s.cfg['proxy']])
            self.assertEqual(list(s.proxy_load.values()),[100,0])
            err=OSError(28,'disk full')
            with patch.object(s,'hf_download',side_effect=err) as mock:
                with self.assertRaises(OSError):s.download(row)
                self.assertEqual(mock.call_count,1)
            self.assertEqual(list(s.proxy_load.values()),[100,0])

    def test_small_uploads_release_slots_and_large_files_remain_fair(self):
        # Immediate downloads make the test's upload queue deterministic, without network or credentials.
        class ImmediatePool:
            def __init__(self,*args,**kwargs): pass
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def submit(self,fn,*args):
                f=cf.Future()
                try: f.set_result(fn(*args))
                except Exception as e: f.set_exception(e)
                return f
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); name=root/'blob'; name.write_bytes(b'x')
            s=Sync.__new__(Sync); s.root=root
            s.cfg=dict(workers=8,upload_workers=1,mode='http',endpoint='test')
            s.transport={}; s.proxy_load={}; s.auth_blocked=threading.Event()
            rows=[dict(path=str(i),size=n) for i,n in enumerate([100,200,1,2,3,4,5,6])]
            waiting=list(rows); done=[]; updates=[]; errors=[]
            s.initialize=lambda:None; s.backup=lambda:None
            def admit(slots):
                result=waiting[:slots];del waiting[:slots];return result
            s.admit=admit;s.download=lambda row:(str(name),'crc')
            s.upload=lambda row,*args:done.append(row['size'])
            s.update=lambda path,**fields:updates.append((path,fields))
            s.failed=lambda row,e:errors.append(type(e).__name__)
            s.summary=lambda:dict(verified_bytes=sum(done),states={'verified':{}} if len(done)==len(rows) else {'pending':{}})
            with patch('sync.cf.ThreadPoolExecutor',ImmediatePool),patch('sync.cf.ProcessPoolExecutor',ImmediatePool), \
                 patch('sync.time.sleep'),patch('sync.time.monotonic',side_effect=range(0,10000,31)):
                s.run()
            self.assertEqual(done,[100,1,2,3,200,4,5,6])
            self.assertEqual(errors,[])
            self.assertEqual(len(updates),len(rows))

    def test_rate_limit_headers_and_nested_error(self):
        cause=RuntimeError('rate limit'); cause.response=SimpleNamespace(status_code=429,headers={'Retry-After':'40','RateLimit':'"resolvers";r=0;t=55'})
        wrapped=RuntimeError('metadata unavailable'); wrapped.__cause__=cause
        code,headers=http_failure(wrapped)
        self.assertEqual(code,429)
        self.assertEqual(rate_limit_delay(headers),57)
        self.assertEqual(rate_limit_delay({}),302)
        with patch('sync.time.time',return_value=0):
            self.assertEqual(rate_limit_delay({'Retry-After':'Thu, 01 Jan 1970 00:01:00 GMT'}),62)

    def test_digest_and_crc(self):
        body=b'checkpoint test'*100
        digest=hashlib.sha256(body).hexdigest()
        crc=fingerprint(io.BytesIO(body),len(body),digest)
        git=hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()
        self.assertEqual(crc,fingerprint(io.BytesIO(body),len(body),git_blob_id=git))
        with self.assertRaises(IntegrityError): fingerprint(io.BytesIO(body+b'x'),len(body),digest)
        self.assertGreater(retry_delay(5),retry_delay(1))
        self.assertLess(retry_delay(1000),1806)

    def test_complete_cannot_overwrite(self):
        b=TargetBucket(oss2.Auth('test','test'),'https://oss-cn-wulanchabu-internal.aliyuncs.com','example-bucket')
        with patch.object(oss2.Bucket,'complete_multipart_upload') as complete:
            b.complete_multipart_upload('datasets/example/test','owned-upload',[])
            self.assertEqual(complete.call_args.kwargs['headers']['x-oss-forbid-overwrite'],'true')

    def test_retry_reuses_download_and_restart_recovers_claim(self):
        body=b'abc'; sha=hashlib.sha256(body).hexdigest()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); cfg=dict(endpoint='https://oss-cn-wulanchabu-internal.aliyuncs.com',
              bucket='example-bucket',prefix='datasets/example/',mode='http',workers=2,upload_workers=1,
              durable=str(root/'durable'),proxy='http://127.0.0.1:1080',repo='example/dataset',
              revision='a'*40,stage_limit_bytes=131078,disk_reserve_bytes=1)
            (root/'settings.json').write_text(json.dumps(cfg))
            objects=[dict(path=f'sim/test{i}.tar.gz',bytes=3,sha256=sha,git_blob_id='b'*40) for i in range(3)]
            (root/'manifest.json').write_text(json.dumps(dict(repo=cfg['repo'],revision=cfg['revision'],files=3,bytes=9,objects=objects)))
            s=Sync(root); s.initialize()
            import huggingface_hub.file_download as fd
            self.assertFalse(fd.is_xet_available())
            first=s.admit(2); self.assertEqual(len(first),2)
            self.assertEqual(s.admit(2),[])
            downloads=[]
            def download(repo,path,**kwargs):
                p=Path(kwargs['cache_dir'])/'blob'; p.parent.mkdir(exist_ok=True); p.write_bytes(body)
                downloads.append(path); return str(p)
            s.hf_download=download
            class Head:
                content_length=3
                headers={'x-oss-hash-crc64ecma':fingerprint(io.BytesIO(body),3,sha)}
            class Fake:
                def head_object(self,key): return Head()
            s.bucket=lambda:Fake()
            s.existing=lambda *args:False
            with patch('sync.oss2.resumable_upload',side_effect=OSError('injected upload interruption')):
                process(s,first[0])
            self.assertEqual(s.summary()['states']['retry']['files'],1)
            s.update(first[0]['path'],next_retry=0)
            retry=s.admit(1)[0]
            s.cfg['upload_part_threads']=4
            with patch('sync.oss2.resumable_upload') as uploaded:
                process(s,retry)
            self.assertEqual(uploaded.call_args.kwargs['num_threads'],4)
            self.assertEqual(downloads,[first[0]['path']])
            self.assertEqual(s.summary()['verified_files'],1)
            self.assertFalse(s.cache_dir(first[0]['path']).exists())
            # A 0.1 task has the same identity but lacks the nullable Xet column.
            with s.connect() as db:db.execute('ALTER TABLE files DROP COLUMN xet_hash')
            s.initialize()
            self.assertEqual(s.summary()['verified_files'],1)
            self.assertEqual(s.summary()['states']['retry']['files'],1)
            self.assertEqual(s.admit(1)[0]['path'],first[1]['path'])
            self.assertTrue((root/'durable/state.sqlite3').exists())
            with self.assertRaises(DestinationConflict):
                s.existing=Sync.existing.__get__(s)
                wrong=dict(first[0]); wrong['size']=5
                s.existing(Fake(),wrong,'datasets/example/test')
            cause=RuntimeError('rate limit'); cause.response=SimpleNamespace(status_code=429,headers={'Retry-After':'30'})
            wrapped=RuntimeError('metadata unavailable'); wrapped.__cause__=cause
            s.existing=lambda *args:False
            with patch.object(s,'hf_download',side_effect=wrapped): process(s,first[1])
            self.assertEqual(s.admit(1),[])
            until=s.rate_limit_until; self.assertGreater(until,time.time()+29)
            s.initialize(); self.assertEqual(s.rate_limit_until,until)
            self.assertEqual(s.admit(1),[])
            with patch('sync.time.time',return_value=until+1):
                self.assertEqual(len(s.admit(1)),1)

    def test_independent_pipeline_and_restart_stages(self):
        body=b'pipeline'; sha=hashlib.sha256(body).hexdigest()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); cfg=dict(endpoint='https://oss-cn-wulanchabu-internal.aliyuncs.com',
                bucket='example-bucket',prefix='datasets/example/',mode='http',workers=1,upload_workers=1,
                durable=str(root/'durable'),proxy='http://127.0.0.1:1080',repo='example/dataset',
                revision='a'*40,stage_limit_bytes=4*(65536+len(body)),disk_reserve_bytes=1)
            (root/'settings.json').write_text(json.dumps(cfg))
            objects=[dict(path=f'test{i}',bytes=len(body),sha256=sha,git_blob_id=None) for i in range(4)]
            (root/'manifest.json').write_text(json.dumps(dict(repo=cfg['repo'],revision=cfg['revision'],files=4,bytes=len(body)*4,objects=objects)))
            s=Sync(root); s.initialize()
            for row,stage in zip(s.admit(4),['checking','ready','uploading','downloading']):
                s.update(row['path'],status=stage)
            s.initialize(); self.assertEqual(s.summary()['states']['retry']['files'],4)
            started=threading.Event(); released=threading.Event(); progressed=threading.Event()
            def download(repo,path,**kwargs):
                if path=='test1' and not started.wait(10): raise TimeoutError('first upload not started')
                if started.is_set() and not released.is_set(): progressed.set()
                name=Path(kwargs['cache_dir'])/'blob'; name.parent.mkdir(exist_ok=True); name.write_bytes(body)
                return str(name)
            def upload(*args,**kwargs):
                started.set()
                if not released.wait(15): raise TimeoutError('test upload not released')
            crc=fingerprint(io.BytesIO(body),len(body),sha)
            s.hf_download=download; s.existing=lambda *args:False
            s.bucket=lambda:SimpleNamespace(head_object=lambda key:SimpleNamespace(content_length=len(body),headers={'x-oss-hash-crc64ecma':crc}))
            errors=[]
            def run():
                try: s.run()
                except Exception as e: errors.append(e)
            # A blocked upload must not hold the only download slot.
            with patch('sync.oss2.resumable_upload',side_effect=upload):
                worker=threading.Thread(target=run); worker.start()
                try:
                    self.assertTrue(started.wait(10))
                    self.assertTrue(progressed.wait(10),'upload blocked further downloads')
                finally:
                    released.set(); worker.join(40)
            self.assertFalse(worker.is_alive()); self.assertEqual(errors,[])
            self.assertEqual(s.summary()['verified_files'],4)
            self.assertEqual(s.summary()['states']['verified']['bytes'],len(body)*4)


if __name__=='__main__': unittest.main()

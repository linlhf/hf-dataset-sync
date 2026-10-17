"""Plan, initialize, observe and reconcile tasks. No production deployment side effects."""
import argparse
import concurrent.futures as cf
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
from urllib.parse import urlsplit

from tasklib import validate_manifest, validate_settings, task_identity, relative_path


def read(path):
    return json.loads(Path(path).read_text())


def write_new(path,data):
    # Exclusive creation prevents a rerun from replacing reviewed plans or state.
    with Path(path).open('x') as f: json.dump(data,f,ensure_ascii=False,indent=2)


def plan(args):
    os.environ['HF_ENDPOINT']='https://huggingface.co'
    from huggingface_hub import HfApi, set_client_factory
    import httpx
    lock=threading.Lock(); deadline=[0.0]
    def pace(request):
        with lock:
            time.sleep(max(0,deadline[0]-time.monotonic()));deadline[0]=time.monotonic()+1
    set_client_factory(lambda:httpx.Client(follow_redirects=True,timeout=60,event_hooks={'request':[pace]}))
    api=HfApi(endpoint='https://huggingface.co',token=os.environ.get('HF_TOKEN') or False)
    kind='bucket' if args.bucket else 'dataset';repo=args.bucket or args.repo
    if kind=='bucket':
        if args.revision is not None:raise ValueError('Buckets do not have Git revisions')
        files=[f for f in api.list_bucket_tree(repo,prefix=args.prefix or None,recursive=True) if f.type=='file']
        revision=hashlib.sha256(json.dumps(sorted((f.path,f.size,f.xet_hash) for f in files),separators=(',',':')).encode()).hexdigest()
    else:
        if args.prefix:raise ValueError('Use --include for repository path selection')
        revision=api.dataset_info(repo,revision=args.revision or 'main').sha
        def scan(path):
            return list(api.list_repo_tree(repo,repo_type='dataset',revision=revision,path_in_repo=path,recursive=True))
        top=list(api.list_repo_tree(repo,repo_type='dataset',revision=revision))
        files=[f for f in top if hasattr(f,'size')]
        with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
            for entries in pool.map(scan,[f.path for f in top if not hasattr(f,'size')]):
                files.extend(f for f in entries if hasattr(f,'size'))
    objects=[];excluded=[]
    for f in sorted(files,key=lambda f:f.path):
        row=({'path':f.path,'bytes':f.size,'sha256':None,'git_blob_id':None,'xet_hash':f.xet_hash} if kind=='bucket' else
             {'path':f.path,'bytes':f.size,'sha256':f.lfs.sha256 if f.lfs else None,'git_blob_id':f.blob_id})
        selected=(not args.include or any(fnmatch.fnmatchcase(f.path,p) for p in args.include)) and not any(fnmatch.fnmatchcase(f.path,p) for p in args.exclude)
        (objects if selected else excluded).append(row)
    manifest={'source_type':kind,'repo':repo,'revision':revision,'files':len(objects),'bytes':sum(r['bytes'] for r in objects),'objects':objects,
              'excluded':excluded,'selection':{'include':args.include,'exclude':args.exclude,'prefix':args.prefix},'raw_files':len(files),'enumerated_at':time.time()}
    validate_manifest(manifest)
    validate_manifest(dict(manifest,objects=objects+excluded,files=len(files),bytes=sum(r['bytes'] for r in objects+excluded)))
    write_new(args.output,manifest)
    return {k:manifest[k] for k in ('repo','revision','files','bytes','raw_files')}


def init(args):
    m=read(args.manifest);validate_manifest(m)
    cfg=read(args.profile)
    u=urlsplit(args.destination)
    if u.scheme!='oss' or not u.netloc or u.query or u.fragment or u.username or u.password: raise ValueError('Expected oss://bucket/dedicated-prefix/')
    prefix=u.path.lstrip('/').rstrip('/')+'/'
    relative_path(prefix[:-1])
    root=Path(args.task).resolve()
    kind=m.get('source_type','dataset');mode=getattr(args,'mode',None) or ('xet' if kind=='bucket' else cfg['mode'])
    cfg.update(repo=m['repo'],revision=m['revision'],source_type=kind,mode=mode,bucket=u.netloc,prefix=prefix,durable=str(root/'durable'))
    if mode=='xet':cfg['download_proxies']=[cfg['proxy']]
    validate_settings(cfg)
    if any(r['bytes']+65536>cfg['stage_limit_bytes'] for r in m['objects']): raise ValueError('File exceeds staging budget')
    root.mkdir(mode=0o700,parents=True,exist_ok=False)
    write_new(root/'manifest.json',m);write_new(root/'settings.json',cfg)
    return {'task':str(root),'files':m['files'],'bytes':m['bytes'],'started':False}


def sample(root):
    with sqlite3.connect('file:'+str(root/'state.sqlite3')+'?mode=ro',uri=True) as db:
        db.execute('BEGIN')
        states={s:{'files':n,'bytes':b} for s,n,b in db.execute('SELECT status,count(*),sum(size) FROM files GROUP BY status')}
        partials=db.execute("SELECT path,size FROM files WHERE reserved=1 AND status NOT IN ('verified','checking','ready','uploading')").fetchall()
    downloaded=sum(states.get(s,{}).get('bytes',0) for s in ('verified','checking','ready','uploading'))
    mode=read(root/'settings.json')['mode']
    # Xet can preallocate full-size files: count completed reconstructions only.
    for path,size in partials if mode=='http' else []:
        n=0
        for p in (root/'cache'/hashlib.sha256(path.encode()).hexdigest()).glob('datasets--*/blobs/*'):
            try: n+=p.stat().st_size if p.is_file() else 0
            except FileNotFoundError: pass
        downloaded+=min(n,size)
    return {'time':time.time(),'states':states,'downloaded_bytes':downloaded,'verified_bytes':states.get('verified',{}).get('bytes',0),
            'download_metric':'completed_reconstruction_bytes' if mode=='xet' else 'effective_download_progress'}


def status(args):
    root=Path(args.task).resolve();m=read(root/'manifest.json');validate_manifest(m)
    a=sample(root);b=a
    if args.seconds:
        time.sleep(args.seconds);b=sample(root);dt=b['time']-a['time']
        b.update(sample_seconds=dt,download_MBs=(b['downloaded_bytes']-a['downloaded_bytes'])/dt/1e6,
                 end_to_end_MBs=(b['verified_bytes']-a['verified_bytes'])/dt/1e6)
    b.update(total_bytes=m['bytes'],remaining_bytes=m['bytes']-b['verified_bytes'],
             percent=100*b['verified_bytes']/m['bytes'] if m['bytes'] else 100)
    return b


def reconcile(m,db_rows,objects,prefix):
    bucket=m.get('source_type')=='bucket'
    expected={r['path']:(r['bytes'],r.get('sha256'),r.get('git_blob_id'))+((r['xet_hash'],) if bucket else ()) for r in m['objects']}
    db={r[0]:tuple(r[1:5] if bucket else r[1:4]) for r in db_rows}
    missing_db=sorted(expected.keys()-db.keys());extra_db=sorted(db.keys()-expected.keys())
    changed=[p for p in expected.keys()&db.keys() if expected[p]!=db[p]]
    seen=set();wrong=[];extra=0
    for key,size in objects:
        path=key[len(prefix):] if key.startswith(prefix) else None
        if path not in expected: extra+=1;continue
        seen.add(path)
        if size!=expected[path][0]:wrong.append(path)
    missing=sorted(expected.keys()-seen)
    return {'selected_files':len(expected),'missing_db':missing_db,'extra_db':extra_db,'changed_db':changed,
            'missing_oss':missing,'size_mismatch_oss':wrong,'unselected_oss_objects':extra,
            'coverage_ok':not any((missing_db,extra_db,changed,missing,wrong)),
            'verification':'full key and size reconciliation; not a fresh content rehash'}


def verify(args):
    import oss2
    root=Path(args.task).resolve();m=read(root/'manifest.json');cfg=read(root/'settings.json')
    validate_manifest(m);validate_settings(cfg)
    with sqlite3.connect('file:'+str(root/'state.sqlite3')+'?mode=ro',uri=True) as db:
        saved=db.execute("SELECT value FROM metadata WHERE key='task_identity'").fetchone()
        if not saved or saved[0]!=task_identity(cfg,m):raise ValueError('Task identity mismatch')
        columns='path,size,sha256,git_blob_id'+(',xet_hash' if m.get('source_type')=='bucket' else '')
        rows=db.execute('SELECT '+columns+' FROM files').fetchall()
        remaining=db.execute("SELECT count(*) FROM files WHERE status!='verified'").fetchone()[0]
    session=oss2.Session();session.session.trust_env=False
    b=oss2.Bucket(oss2.Auth(os.environ['OSS_ACCESS_KEY_ID'],os.environ['OSS_ACCESS_KEY_SECRET']),cfg['endpoint'],cfg['bucket'],session=session)
    result=reconcile(m,rows,((o.key,o.size) for o in oss2.ObjectIteratorV2(b,prefix=cfg['prefix'])),cfg['prefix'])
    result.update(unverified_files=remaining,complete=result['coverage_ok'] and remaining==0)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    q=sub.add_parser('plan');g=q.add_mutually_exclusive_group(required=True);g.add_argument('--repo');g.add_argument('--bucket')
    q.add_argument('--revision');q.add_argument('--prefix',default='');q.add_argument('--output',required=True)
    q.add_argument('--workers',type=int,choices=range(1,9),default=4);q.add_argument('--include',action='append',default=[]);q.add_argument('--exclude',action='append',default=[])
    q=sub.add_parser('init');q.add_argument('--manifest',required=True);q.add_argument('--profile',required=True);q.add_argument('--destination',required=True);q.add_argument('--task',required=True)
    q.add_argument('--mode',choices=['http','xet'])
    for name in ('status','verify'):
        q=sub.add_parser(name);q.add_argument('--task',required=True)
        if name=='status':q.add_argument('--seconds',type=int,choices=range(0,121),default=0)
    args=p.parse_args()
    try: result=globals()[args.command](args)
    except Exception as e:
        # SDK errors can contain signed URLs or credentials; only report the type.
        print(json.dumps({'error_type':type(e).__name__,'command':args.command}),file=sys.stderr);return 1
    print(json.dumps(result,ensure_ascii=False,indent=2))
    return 2 if args.command=='verify' and not result['complete'] else 0


if __name__=='__main__':raise SystemExit(main())

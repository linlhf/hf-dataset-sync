"""Validation shared by planning, execution and read-only reconciliation."""
import hashlib
import json
import re
from urllib.parse import urlsplit


def relative_path(value):
    if not isinstance(value,str) or not value or any(ord(c)<32 for c in value) or '\\' in value:
        raise ValueError('Invalid relative path')
    if any(p in ('','.','..') for p in value.split('/')):
        raise ValueError('Path must be canonical and relative')
    return value


def validate_manifest(m):
    if not re.fullmatch(r'[\w.-]+/[\w.-]+',m['repo']) or not re.fullmatch(r'[a-f0-9]{40}',m['revision']):
        raise ValueError('Expected dataset repo ID and pinned commit SHA')
    seen=set()
    for row in m['objects']:
        path=relative_path(row['path'])
        if path in seen: raise ValueError('Duplicate manifest path')
        seen.add(path)
        if type(row['bytes']) is not int or row['bytes']<0: raise ValueError('Invalid size')
        sha=row.get('sha256'); git=row.get('git_blob_id')
        if not ((sha and re.fullmatch(r'[a-f0-9]{64}',sha)) or (not sha and git and re.fullmatch(r'[a-f0-9]{40}',git))):
            raise ValueError('Missing or invalid source digest')
    if not seen or len(seen)!=m['files'] or sum(r['bytes'] for r in m['objects'])!=m['bytes']:
        raise ValueError('Manifest totals mismatch or empty selection')


def validate_settings(cfg):
    if not re.fullmatch(r'https://oss-[a-z0-9-]+-internal\.aliyuncs\.com',cfg['endpoint']):
        raise ValueError('Use the target bucket region internal HTTPS endpoint')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{1,61}[a-z0-9]',cfg['bucket']):
        raise ValueError('Invalid bucket name')
    prefix=cfg['prefix']
    if not prefix.endswith('/'): raise ValueError('Require a dedicated nonempty prefix ending in /')
    relative_path(prefix[:-1])
    if cfg['mode']!='http': raise ValueError('This release supports official HF HTTP downloads only')
    for key,limit in [('workers',64),('upload_workers',32),('upload_part_threads',8),('max_attempts',100)]:
        value=cfg.get(key,{'upload_part_threads':2,'max_attempts':12}.get(key))
        if type(value) is not int or not 1<=value<=limit: raise ValueError('Invalid '+key)
    if not 0<float(cfg.get('hf_requests_per_second',10))<=16: raise ValueError('Invalid HF request rate')
    if cfg['stage_limit_bytes']<=0 or cfg['disk_reserve_bytes']<0: raise ValueError('Invalid disk budget')
    from pathlib import Path
    if not Path(cfg['durable']).is_absolute(): raise ValueError('Durable directory must be absolute')
    proxies=cfg.get('download_proxies',[cfg['proxy']])
    if not proxies or len(proxies)!=len(set(proxies)): raise ValueError('Invalid proxy list')
    for proxy in [cfg['proxy'],*proxies]:
        if proxy=='': continue
        u=urlsplit(proxy)
        if u.scheme!='http' or u.hostname not in ('127.0.0.1','localhost','::1') or not u.port or u.username or u.password or u.query or u.fragment or u.path not in ('','/'):
            raise ValueError('Only credential-free loopback HTTP proxies or empty direct route are supported')


def task_identity(cfg,m):
    rows=sorted((r['path'],r['bytes'],r.get('sha256'),r.get('git_blob_id')) for r in m['objects'])
    data=[m['repo'],m['revision'],cfg['endpoint'],cfg['bucket'],cfg['prefix'],rows]
    return hashlib.sha256(json.dumps(data,separators=(',',':')).encode()).hexdigest()

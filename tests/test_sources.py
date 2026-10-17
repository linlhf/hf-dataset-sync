import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from huggingface_hub import HfApi
from huggingface_hub._buckets import BucketFile
from hf_xet import hash_files
from sync import Sync, SourceChanged, IntegrityError, fingerprint_file
from task import reconcile, sample, plan, init
from tasklib import validate_manifest,validate_settings,task_identity,source_digest


class SourcesTest(unittest.TestCase):
    def setUp(self):
        env=patch.dict(os.environ,{'HF_TOKEN':'test','OSS_ACCESS_KEY_ID':'test','OSS_ACCESS_KEY_SECRET':'test'})
        env.start();self.addCleanup(env.stop)

    def task(self,root,body=b'abc'):
        f=root/'original';f.write_bytes(body);digest=hash_files([str(f)])[0].hash
        m={'source_type':'bucket','repo':'example/bucket','revision':'a'*64,'files':1,'bytes':len(body),
           'objects':[{'path':'data/file','bytes':len(body),'sha256':None,'git_blob_id':None,'xet_hash':digest}]}
        cfg=json.loads((Path(__file__).resolve().parents[1]/'examples/profile.json').read_text())
        cfg.update(source_type='bucket',mode='xet',repo=m['repo'],revision=m['revision'],bucket='example-bucket',prefix='data/',durable=str(root/'durable'))
        (root/'manifest.json').write_text(json.dumps(m));(root/'settings.json').write_text(json.dumps(cfg))
        return cfg,m

    def test_bucket_native_hash_is_not_sha256(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);cfg,m=self.task(root);row=m['objects'][0]
            validate_manifest(m);validate_settings(cfg)
            self.assertNotEqual(row['xet_hash'],hashlib.sha256(b'abc').hexdigest())
            self.assertTrue(fingerprint_file(root/'original',3,None,None,row['xet_hash']))
            (root/'original').write_bytes(b'abd')
            with self.assertRaises(IntegrityError):fingerprint_file(root/'original',3,None,None,row['xet_hash'])
            before=task_identity(cfg,m);row['xet_hash']='b'*64
            self.assertNotEqual(before,task_identity(cfg,m))

    def test_bucket_download_pins_file_and_detects_drift(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);cfg,m=self.task(root);s=Sync(root);s.initialize();row=s.admit(1)[0]
            s.bucket=lambda:None;s.existing=lambda *a:False
            f=BucketFile(type='file',path=row['path'],size=3,xetHash=row['xet_hash'])
            def download(bucket,files,**kw):
                self.assertTrue(kw['raise_on_missing_files']);self.assertEqual(files[0][0].xet_hash,row['xet_hash'])
                Path(files[0][1]).write_bytes(b'abc')
            with patch.object(HfApi,'get_bucket_paths_info',return_value=[f]),patch.object(HfApi,'download_bucket_files',side_effect=download):
                name,crc=s.download(row)
            self.assertIsNone(crc);self.assertEqual(Path(name).read_bytes(),b'abc')
            self.assertEqual(s.transport['bucket_xet_calls'],1)
            for infos in [[],[BucketFile(type='file',path=row['path'],size=3,xetHash='b'*64)]]:
                with patch.object(HfApi,'get_bucket_paths_info',return_value=infos):
                    with self.assertRaises(SourceChanged):s.download(row)
            s.failed(row,SourceChanged('drift'));self.assertEqual(s.summary()['states']['blocked']['files'],1)

    def test_xet_mode_and_preallocation_accounting(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);cfg,m=self.task(root);s=Sync(root);s.initialize();row=s.admit(1)[0]
            self.assertEqual(os.environ['https_proxy'],cfg['proxy'])
            self.assertEqual(os.environ['HTTPS_PROXY'],cfg['proxy'])
            self.assertEqual(os.environ['NO_PROXY'],'')
            p=s.cache_dir(row['path'])/'bucket-content';p.parent.mkdir();p.write_bytes(b'\0'*3)
            self.assertEqual(sample(root)['downloaded_bytes'],0)
            self.assertEqual(sample(root)['download_metric'],'completed_reconstruction_bytes')
            s.update(row['path'],status='checking')
            self.assertEqual(sample(root)['downloaded_bytes'],3)
            cfg['download_proxies']=['http://127.0.0.1:1082','http://127.0.0.1:1083']
            with self.assertRaises(ValueError):validate_settings(cfg)

    def test_bucket_reconcile_includes_xet_and_zero_byte_hash(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);cfg,m=self.task(root,b'');r=m['objects'][0]
            self.assertTrue(fingerprint_file(root/'original',0,None,None,r['xet_hash']))
            rows=[(r['path'],0,None,None,r['xet_hash'])]
            self.assertTrue(reconcile(m,rows,[('out/'+r['path'],0)],'out/')['coverage_ok'])
            rows[0]=(r['path'],0,None,None,'b'*64)
            self.assertFalse(reconcile(m,rows,[('out/'+r['path'],0)],'out/')['coverage_ok'])
            self.assertEqual(source_digest(r),'xet:'+r['xet_hash'])

    def test_bucket_plan_filters_inventory_and_init_selects_xet(self):
        from huggingface_hub import set_client_factory
        from huggingface_hub.utils._http import default_client_factory
        self.addCleanup(set_client_factory,default_client_factory)
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            args=SimpleNamespace(bucket='example/bucket',repo=None,revision=None,prefix='data/',include=[],exclude=['data/old'],output=root/'plan.json')
            files=[BucketFile(type='file',path=p,size=3,xetHash=h*64) for p,h in [('data/new','a'),('data/old','b')]]
            with patch.object(HfApi,'list_bucket_tree',return_value=iter(files)) as listing:
                result=plan(args)
            listing.assert_called_once_with('example/bucket',prefix='data/',recursive=True)
            self.assertEqual((result['files'],result['bytes'],result['raw_files']),(1,3,2))
            m=json.loads(args.output.read_text());self.assertEqual(m['excluded'][0]['path'],'data/old')
            init(SimpleNamespace(manifest=args.output,profile=Path(__file__).resolve().parents[1]/'examples/profile.json',destination='oss://example-bucket/out/',task=root/'task',mode=None))
            cfg=json.loads((root/'task/settings.json').read_text());self.assertEqual(cfg['mode'],'xet')
            self.assertEqual(cfg['download_proxies'],[cfg['proxy']])
            args.revision='main'
            with self.assertRaises(ValueError):plan(args)


if __name__=='__main__':unittest.main()

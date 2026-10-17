import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from task import init, reconcile, write_new
from tasklib import relative_path, validate_manifest, validate_settings, task_identity


class TaskTest(unittest.TestCase):
    def setUp(self):
        self.manifest={'repo':'example/dataset','revision':'a'*40,'files':1,'bytes':3,
                       'objects':[{'path':'nested/file','bytes':3,'sha256':'b'*64,'git_blob_id':None}]}
        self.cfg=json.loads((Path(__file__).resolve().parents[1]/'examples/profile.json').read_text())
        self.cfg.update(bucket='example-bucket',prefix='datasets/example/',durable='/data/test',repo='example/dataset',revision='a'*40)

    def test_paths_and_manifest(self):
        validate_manifest(self.manifest)
        for path in ['/absolute','../escape','a/../b','a//b','a\\b','a\x00b','./a','']:
            with self.assertRaises(ValueError):relative_path(path)
        m=copy.deepcopy(self.manifest);m['objects']*=2;m.update(files=2,bytes=6)
        with self.assertRaises(ValueError):validate_manifest(m)
        m=copy.deepcopy(self.manifest);m['bytes']=9
        with self.assertRaises(ValueError):validate_manifest(m)

    def test_settings_and_identity(self):
        validate_settings(self.cfg)
        for key,value in [('endpoint','https://oss-cn-beijing.aliyuncs.com'),('prefix','/'),('mode','xet'),('workers',0),('download_proxies',['http://user:secret@127.0.0.1:8080'])]:
            cfg=dict(self.cfg);cfg[key]=value
            with self.assertRaises(ValueError):validate_settings(cfg)
        a=task_identity(self.cfg,self.manifest)
        cfg=dict(self.cfg,workers=48)
        self.assertEqual(a,task_identity(cfg,self.manifest))
        self.assertNotEqual(a,task_identity(dict(cfg,prefix='different/'),self.manifest))
        m=copy.deepcopy(self.manifest);m['objects'][0]['sha256']='c'*64
        self.assertNotEqual(a,task_identity(cfg,m))

    def test_init_exclusive_no_secrets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);write_new(root/'manifest.json',self.manifest);write_new(root/'profile.json',self.cfg)
            args=SimpleNamespace(manifest=root/'manifest.json',profile=root/'profile.json',task=root/'task',destination='oss://example-bucket/test/')
            result=init(args);self.assertFalse(result['started'])
            self.assertEqual({p.name for p in (root/'task').iterdir()},{'manifest.json','settings.json'})
            with self.assertRaises(FileExistsError):init(args)

    def test_reconcile_reports_missing_changed_and_extras(self):
        rows=[('nested/file',3,'b'*64,None)]
        out=reconcile(self.manifest,rows,[('prefix/nested/file',3),('prefix/extra',0)],'prefix/')
        self.assertTrue(out['coverage_ok']);self.assertEqual(out['unselected_oss_objects'],1)
        out=reconcile(self.manifest,rows,[],'prefix/')
        self.assertFalse(out['coverage_ok']);self.assertEqual(out['missing_oss'],['nested/file'])
        out=reconcile(self.manifest,[('nested/file',4,'b'*64,None)],[('prefix/nested/file',4)],'prefix/')
        self.assertEqual(out['changed_db'],['nested/file']);self.assertEqual(out['size_mismatch_oss'],['nested/file'])


if __name__=='__main__':unittest.main()

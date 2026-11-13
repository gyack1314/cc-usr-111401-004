"""领域资料及现有接口核对。"""
import json,tempfile,unittest
from pathlib import Path
from warehouse.objects import ObjectStore
from warehouse.backup import inspect

class DomainTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.store=ObjectStore(self.root/'objects')
        self.data=json.loads(Path('data/source.json').read_text(encoding='utf-8'))
    def test_existing_reader(self):
        result=inspect(self.data,self.store,self.root)
        self.assertTrue(result)

    def test_archive_roundtrip(self):
        from warehouse.backup import create_archive,read_archive
        digest=self.store.put(b'content');path=self.root/'full.tar';create_archive(self.store,[digest],{'base_commit':'c0'},path)
        index,objects=read_archive(path);self.assertEqual(objects[digest],b'content');self.assertEqual(index['base_commit'],'c0')

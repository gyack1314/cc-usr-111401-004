"""按已提交位置恢复：链核对、完整提交边界、崩溃续做与真实查询校验。"""
import copy
import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from warehouse.backup import create_archive, read_archive
from warehouse.objects import ObjectStore, encode
from warehouse.restore import (
    RestoreError, RestoreRun, build_source, completed_stages, open_restored,
    plan_restore, restore, restore_document, stage_status, verify_restored,
)
from warehouse.snapshot import Snapshots


class RestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = json.loads(Path('data/source.json').read_text(encoding='utf-8'))

    def source(self, data=None, name='source'):
        return build_source(data if data is not None else self.data,
                            self.root / name)

    def test_restore_each_target(self):
        expected = {
            'c0': {'usage': 1, 'teams': 10},
            'c1': {'usage': 2, 'teams': 11},
            'c2': {'usage': 3, 'teams': 11},
        }
        for target, values in expected.items():
            with self.subTest(target=target):
                out = self.root / ('out-' + target)
                result = restore_document(self.data, target, out,
                                          workdir=self.root / ('src-' + target))
                self.assertEqual(result['target'], target)
                self.assertEqual(result['verification']['target'], target)
                warehouse = open_restored(out)
                self.assertEqual(warehouse.tables, ['teams', 'usage'])
                for table, value in values.items():
                    rows = warehouse.query(table)
                    self.assertEqual([row['value'] for row in rows], [value])
                    self.assertEqual([row['row_id'] for row in rows], ['a'])
                # 校验材料包含每张表的字段目录、摘要与引用来源。
                for check in result['tables']:
                    # 字段目录：schema 字段在前，行身份字段其后。
                    self.assertEqual(
                        [field['name'] for field in check['fields']],
                        list(check['schema']) + ['row_id'])
                    self.assertIn('row_id', [f['name'] for f in check['fields']])
                    self.assertRegex(check['snapshot'], r'^[0-9a-f]{64}$')
                    self.assertRegex(check['data_digest'], r'^[0-9a-f]{64}$')
                    self.assertEqual(check['references']['target'], target)
                    self.assertIn(target, check['references']['commits'])

    def test_verification_material_files(self):
        out = self.root / 'out'
        result = restore_document(self.data, 'c2', out, workdir=self.root / 'src')
        for name in ('manifest.json', 'catalog.json', 'refs.json', 'index.json',
                     'verification.json', 'warehouse.sqlite3'):
            self.assertTrue((out / name).is_file(), name)
        verification = json.loads((out / 'verification.json').read_text())
        self.assertEqual(verification['plan_digest'], result['plan_digest'])
        self.assertEqual(len(verification['tables']), 2)
        catalog = json.loads((out / 'catalog.json').read_text())
        self.assertEqual(catalog['tables']['usage']['schema'], {'value': 'int'})
        self.assertRegex(catalog['tables']['usage']['data'], r'^[0-9a-f]{64}$')

    def test_multi_table_commit_is_atomic(self):
        result = restore_document(self.data, 'c1', self.root / 'out',
                                  workdir=self.root / 'src')
        self.assertEqual(result['rows']['usage'][0]['value'], 2)
        self.assertEqual(result['rows']['teams'][0]['value'], 11)

    def test_target_excludes_later_delete_and_schema(self):
        data = copy.deepcopy(self.data)
        data['tables']['t2'] = {'schema': {'value': 'int', 'extra': 'str'},
                                'rows': [{'row_id': 'a', 'value': 12, 'extra': 'x'}]}
        data['tables']['u3'] = {'schema': {'value': 'str'},
                                'rows': [{'row_id': 'a', 'value': 'three'}]}
        data['increments'] = [
            {'sequence': 1, 'commit_id': 'c1',
             'changes': {'usage': 'u1', 'teams': 't1'}},
            {'sequence': 2, 'commit_id': 'c2', 'changes': {'usage': 'u2'}},
            {'sequence': 3, 'commit_id': 'c3',
             'changes': {'teams': None, 'audit': 't2'}},
            {'sequence': 4, 'commit_id': 'c4', 'changes': {'usage': 'u3'}},
        ]
        out = self.root / 'c2'
        restore_document(data, 'c2', out, workdir=self.root / 'src2')
        warehouse = open_restored(out)
        self.assertEqual(warehouse.tables, ['teams', 'usage'])
        self.assertEqual(warehouse.schema('usage'), {'value': 'int'})
        self.assertEqual(warehouse.query('teams')[0]['value'], 11)
        self.assertEqual(warehouse.query('usage')[0]['value'], 3)
        # 目标之后的删除与新表确实被忽略。
        out3 = self.root / 'c3'
        result = restore_document(data, 'c3', out3, workdir=self.root / 'src3')
        self.assertEqual(open_restored(out3).tables, ['audit', 'usage'])
        self.assertEqual(result['rows']['audit'][0],
                         {'row_id': 'a', 'value': 12, 'extra': 'x'})

    def test_broken_link_reports_end_and_blocker(self):
        data = copy.deepcopy(self.data)
        data['increments'][1]['parent_digest'] = '0' * 64
        source = self.source(data, 'src')
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source['store'], 'c2', source['archive'], source['log'])
        report = caught.exception.report
        self.assertEqual(report['recoverable_end'], 'c1')
        blocked = report['blocked']
        self.assertEqual(blocked['stage'], 'log')
        self.assertEqual(blocked['commit_id'], 'c2')
        self.assertEqual(blocked['reason'], '父摘要链接断裂')
        self.assertEqual(blocked['position'], 1)
        # 断裂前的提交仍可恢复，且目标之后的断裂不影响更早的目标。
        result = restore(source['store'], 'c1', source['archive'], source['log'],
                         self.root / 'c1')
        self.assertEqual(result['target'], 'c1')
        self.assertEqual(result['rows']['usage'][0]['value'], 2)

    def test_sequence_gap_is_blocked(self):
        data = copy.deepcopy(self.data)
        data['increments'][1]['sequence'] = 7
        source = self.source(data, 'src')
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source['store'], 'c2', source['archive'], source['log'])
        self.assertEqual(caught.exception.report['recoverable_end'], 'c1')
        self.assertEqual(caught.exception.report['blocked']['reason'], '日志编号不连续')

    def test_baseline_mismatch_is_blocked(self):
        source = self.source(name='src')
        other = dict(source['archive'])
        other['base_commit'] = 'cX'
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source['store'], 'c1', other, source['log'])
        self.assertEqual(caught.exception.report['blocked']['stage'], 'baseline')
        # 日志第一条指向另一个基线时也明确指出起点不匹配。
        data = copy.deepcopy(self.data)
        data['increments'][0]['parent_digest'] = '1' * 64
        source2 = self.source(data, 'src2')
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source2['store'], 'c1', source2['archive'], source2['log'])
        self.assertIn('基线', caught.exception.report['blocked']['reason'])
        self.assertEqual(caught.exception.report['recoverable_end'], 'c0')

    def test_archive_tamper_is_blocked(self):
        import io
        import tarfile
        source = self.source(name='src')
        path = Path(source['archive']['path'])
        with tarfile.open(path, 'r:') as archive:
            entries = {member.name: archive.extractfile(member).read()
                       for member in archive.getmembers()}
        target = next(name for name in entries if name.startswith('objects/'))
        entries[target] = entries[target] + b' '
        with tarfile.open(path, 'w') as archive:
            for name, raw in entries.items():
                info = tarfile.TarInfo(name)
                info.size = len(raw)
                info.mtime = 0
                info.mode = 0o600
                archive.addfile(info, io.BytesIO(raw))
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source['store'], 'c1', source['archive'], source['log'])
        self.assertEqual(caught.exception.report['blocked']['stage'], 'baseline')

    def test_missing_objects_report_blocker(self):
        # 增量提交的一整笔变更必须完整可物化。
        source = self.source(name='src')
        head = source['log'][1]['changes']['usage']
        (source['store'].root / head).unlink()
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source['store'], 'c2', source['archive'], source['log'])
        report = caught.exception.report
        self.assertEqual(report['recoverable_end'], 'c1')
        self.assertEqual(report['blocked']['stage'], 'commit')
        self.assertEqual(report['blocked']['commit_id'], 'c2')
        # 数据对象缺失同样在计划阶段指出。
        source2 = self.source(name='src2')
        head = source2['log'][1]['changes']['usage']
        manifest = json.loads(source2['store'].get(head))
        (source2['store'].root / manifest['data']).unlink()
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source2['store'], 'c2', source2['archive'], source2['log'])
        self.assertEqual(caught.exception.report['recoverable_end'], 'c1')

    def test_missing_object_does_not_block_earlier_target(self):
        source = self.source(name='src')
        head = source['log'][1]['changes']['usage']
        (source['store'].root / head).unlink()
        result = restore(source['store'], 'c1', source['archive'], source['log'],
                         self.root / 'c1')
        self.assertEqual(result['rows']['usage'][0]['value'], 2)

    def test_fallback_is_explicit(self):
        source = self.source(name='src')
        head = source['log'][1]['changes']['usage']
        (source['store'].root / head).unlink()
        with self.assertRaises(RestoreError):
            restore(source['store'], 'c2', source['archive'], source['log'],
                    self.root / 'strict')
        self.assertFalse((self.root / 'strict').exists())
        result = restore(source['store'], 'c2', source['archive'], source['log'],
                         self.root / 'fallback', allow_fallback=True)
        self.assertEqual(result['target'], 'c1')
        self.assertEqual(result['requested_target'], 'c2')

    def test_crash_resume_recognises_completed_stages(self):
        source = self.source(name='src')
        plan = plan_restore(source['store'], 'c2', source['archive'], source['log'])
        out = self.root / 'out'
        run = RestoreRun(plan, source['store'], out)
        run.stage_objects()
        run.materialize()
        status = stage_status(out, plan['plan_digest'])
        self.assertTrue(status['objects']['complete'])
        self.assertTrue(status['tables']['complete'])
        self.assertFalse(status['manifest']['complete'])
        self.assertFalse(status['verified']['complete'])
        self.assertEqual(completed_stages(out, plan['plan_digest']),
                         ['objects', 'tables'])
        # 阶段产物完整但标记丢失（崩溃在写标记之前）也能识别。
        marker = out / '.restore-state' / plan['plan_digest'] / 'tables.json'
        marker.unlink()
        self.assertTrue(run.stage_complete('tables')[0])
        result = run.run()
        self.assertEqual(result['target'], 'c2')
        self.assertTrue(all(item['complete'] for item in result['stages'].values()))

    def test_rerun_repairs_tampered_artifacts(self):
        source = self.source(name='src')
        plan = plan_restore(source['store'], 'c2', source['archive'], source['log'])
        out = self.root / 'out'
        RestoreRun(plan, source['store'], out).run()
        table_file = out / 'tables' / 'usage.json'
        value = json.loads(table_file.read_text())
        value['rows'][0]['value'] = 999
        table_file.write_text(json.dumps(value))
        run = RestoreRun(plan, source['store'], out)
        self.assertFalse(run.stage_complete('tables')[0])
        result = run.run()
        self.assertEqual(result['rows']['usage'][0]['value'], 3)
        self.assertTrue(run.stage_complete('tables')[0])

    def test_switching_target_does_not_reuse_other_plan_markers(self):
        source = self.source(name='src')
        out = self.root / 'out'
        plan_c2 = plan_restore(source['store'], 'c2', source['archive'], source['log'])
        plan_c1 = plan_restore(source['store'], 'c1', source['archive'], source['log'])
        self.assertNotEqual(plan_c2['plan_digest'], plan_c1['plan_digest'])
        RestoreRun(plan_c2, source['store'], out).run()
        self.assertEqual(completed_stages(out, plan_c1['plan_digest']), [])
        self.assertEqual(plan_c1['steps'][-1]['commit_id'], 'c1')
        result = RestoreRun(plan_c1, source['store'], out).run()
        self.assertEqual(result['target'], 'c1')
        self.assertEqual(result['rows']['usage'][0]['value'], 2)
        self.assertEqual(open_restored(out).target, 'c1')

    def test_plan_is_bound_to_target_and_content(self):
        source = self.source(name='src')
        plan = plan_restore(source['store'], 'c2', source['archive'], source['log'])
        tampered = json.loads(json.dumps(plan))
        tampered['target'] = 'c1'
        with self.assertRaises(RestoreError):
            RestoreRun(tampered, source['store'], self.root / 'out')

    def test_source_store_untouched(self):
        source = self.source(name='src')
        before = {path.name: path.read_bytes()
                  for path in source['store'].root.iterdir()}
        archive_before = Path(source['archive']['path']).read_bytes()
        restore(source['store'], 'c2', source['archive'], source['log'],
                self.root / 'out')
        after = {path.name: path.read_bytes()
                 for path in source['store'].root.iterdir()}
        self.assertEqual(before, after)
        self.assertEqual(archive_before, Path(source['archive']['path']).read_bytes())
        # 恢复目录不得落在来源仓库内部。
        with self.assertRaises(RestoreError):
            restore(source['store'], 'c0', source['archive'], source['log'],
                    source['store'].root / 'inside')

    def test_reused_objects_are_reverified(self):
        source = self.source(name='src')
        out = self.root / 'out'
        RestoreRun(plan_restore(source['store'], 'c2', source['archive'],
                                source['log']), source['store'], out).run()
        # 篡改恢复目录里的内容对象后，续做会重新校验并修复。
        head = plan_restore(source['store'], 'c2', source['archive'],
                            source['log'])['tables']['usage']['snapshot']
        (out / 'objects' / head).write_bytes(b'{}')
        source_plan = plan_restore(source['store'], 'c2', source['archive'],
                                   source['log'])
        run = RestoreRun(source_plan, source['store'], out)
        self.assertFalse(run.stage_complete('objects')[0])
        result = run.run()
        self.assertEqual(result['rows']['usage'][0]['value'], 3)
        self.assertEqual(json.loads((out / 'objects' / head).read_text())['format'], 1)

    def test_database_tamper_is_caught_by_query(self):
        out = self.root / 'out'
        restore_document(self.data, 'c2', out, workdir=self.root / 'src')
        connection = sqlite3.connect(out / 'warehouse.sqlite3')
        connection.execute('UPDATE usage SET value = 999')
        connection.commit()
        connection.close()
        with self.assertRaises(RestoreError) as caught:
            verify_restored(out)
        self.assertEqual(caught.exception.report['blocked']['table'], 'usage')

    def test_report_exposes_end_and_blocker_for_each_failure(self):
        scenarios = []
        data = copy.deepcopy(self.data)
        data['increments'][0]['parent_digest'] = '1' * 64
        scenarios.append(('baseline', data, 'c1', 'c0', '基线'))
        data = copy.deepcopy(self.data)
        data['increments'][1]['sequence'] = 9
        scenarios.append(('sequence', data, 'c2', 'c1', '日志编号不连续'))
        data = copy.deepcopy(self.data)
        data['increments'][1]['commit_id'] = 'c1'
        scenarios.append(('duplicate', data, 'c2', 'c1', '提交身份重复'))
        for name, value, target, end, reason in scenarios:
            with self.subTest(scenario=name):
                source = self.source(value, 'src-' + name)
                with self.assertRaises(RestoreError) as caught:
                    plan_restore(source['store'], target, source['archive'],
                                 source['log'])
                report = caught.exception.report
                self.assertEqual(report.get('recoverable_end'), end)
                self.assertIn(reason, report['blocked']['reason'])
                self.assertIn('blocked', report)

    def test_missing_data_object_names_object(self):
        source = self.source(name='src')
        head = source['log'][1]['changes']['usage']
        manifest = json.loads(source['store'].get(head))
        (source['store'].root / manifest['data']).unlink()
        with self.assertRaises(RestoreError) as caught:
            plan_restore(source['store'], 'c2', source['archive'], source['log'])
        blocked = caught.exception.report['blocked']
        self.assertIn(manifest['data'], blocked['missing'])
        self.assertEqual(blocked['broken'][0]['table'], 'usage')
        self.assertEqual(blocked['commit_id'], 'c2')

    def test_verification_requires_real_database_not_counts(self):
        out = self.root / 'out'
        restore_document(self.data, 'c2', out, workdir=self.root / 'src')
        self.assertTrue((out / 'warehouse.sqlite3').is_file())
        self.assertEqual(open_restored(out).query('usage'),
                         [{'row_id': 'a', 'value': 3}])
        # 只把行数对上但内容不同，仍然无法通过校验。
        connection = sqlite3.connect(out / 'warehouse.sqlite3')
        connection.execute('UPDATE usage SET row_id = \'b\'')
        connection.commit()
        connection.close()
        with self.assertRaises(RestoreError):
            verify_restored(out)
        self.assertEqual(len(open_restored(out).query('usage')), 1)


if __name__ == '__main__':
    unittest.main()

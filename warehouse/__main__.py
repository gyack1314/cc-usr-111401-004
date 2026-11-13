"""资料读取与按提交位置恢复的入口。"""
import argparse
import json
import sys
import tempfile
from pathlib import Path

from .objects import ObjectStore
from .backup import inspect
from .restore import (RestoreError, build_source, completed_stages,
                      load_plan, open_restored, plan_from_files, plan_restore,
                      restore_document, restore_from_files, save_plan,
                      stage_status, verify_restored)


def _load_document(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _option(args, name, default=None):
    return getattr(args, name, default)


def _inputs(args):
    """命令行输入：资料文档构造来源，或归档+日志+对象目录三件套。"""
    archive = _option(args, 'archive')
    log = _option(args, 'log')
    objects = _option(args, 'objects')
    if archive or log or objects:
        if not (archive and log and objects):
            raise SystemExit('--archive、--log、--objects 必须同时提供')
        return {'mode': 'files', 'archive': archive, 'log': log,
                'objects': objects}
    source = _option(args, 'source') or 'data/source.json'
    return {'mode': 'document', 'data': _load_document(source),
            'workdir': _option(args, 'workdir')}


def _plan_for(args, inputs):
    if inputs['mode'] == 'files':
        return plan_from_files(inputs['archive'], inputs['log'], inputs['objects'],
                               args.target, allow_fallback=args.allow_fallback)
    workdir = Path(inputs['workdir']) if inputs['workdir'] else tempfile.mkdtemp(
        prefix='warehouse-source-')
    source = build_source(inputs['data'], workdir)
    return plan_restore(source['store'], args.target, source['archive'],
                        source['log'], allow_fallback=args.allow_fallback)


def command_inspect(args):
    data = _load_document(_option(args, 'source') or 'data/source.json')
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        store = ObjectStore(root / 'objects')
        return inspect(data, store, root), 0


def command_plan(args):
    inputs = _inputs(args)
    try:
        plan = _plan_for(args, inputs)
    except RestoreError as error:
        return {'ok': False, 'error': str(error),
                'report': error.report}, 2
    if args.save:
        save_plan(plan, args.save)
    result = {'ok': True, 'plan_digest': plan['plan_digest'],
              'target': plan['target'],
              'requested_target': plan.get('requested_target'),
              'table_heads': plan['table_heads'],
              'applied': plan['report']['applied'],
              'ignored': plan['report']['ignored'],
              'recoverable_end': plan['report'].get('recoverable_end'),
              'report': plan['report']}
    if args.save:
        result['plan'] = plan
    return result, 0


def command_restore(args):
    inputs = _inputs(args)
    destination = Path(args.directory)
    try:
        if inputs['mode'] == 'files':
            result = restore_from_files(inputs['archive'], inputs['log'],
                                        inputs['objects'], args.target, destination,
                                        allow_fallback=args.allow_fallback)
        else:
            workdir = (Path(inputs['workdir']) if inputs['workdir']
                       else tempfile.mkdtemp(prefix='warehouse-source-'))
            result = restore_document(inputs['data'], args.target, destination,
                                      workdir=workdir,
                                      allow_fallback=args.allow_fallback)
    except RestoreError as error:
        return {'ok': False, 'error': str(error), 'report': error.report}, 2
    return {'ok': True, 'target': result['target'],
            'requested_target': result.get('requested_target'),
            'plan_digest': result['plan_digest'],
            'tables': {check['table']: check['rows']
                       for check in result['tables']},
            'stages': {stage: item['complete']
                       for stage, item in result['stages'].items()},
            'directory': str(destination)}, 0


def command_status(args):
    plan = load_plan(args.plan)
    return {'plan_digest': plan['plan_digest'], 'target': plan['target'],
            'stages': stage_status(args.directory, plan['plan_digest']),
            'completed': completed_stages(args.directory,
                                          plan['plan_digest'])}, 0


def command_open(args):
    warehouse = open_restored(args.directory)
    if args.table:
        if args.table not in warehouse.tables:
            return {'ok': False, 'error': '表不存在',
                    'tables': warehouse.tables}, 2
        return {'ok': True, 'target': warehouse.target, 'table': args.table,
                'schema': warehouse.schema(args.table),
                'fields': warehouse.fields(args.table),
                'rows': warehouse.query(args.table)}, 0
    return {'ok': True, 'target': warehouse.target, 'tables': warehouse.tables,
            'catalog': warehouse.catalog['tables']}, 0


def command_verify(args):
    try:
        return verify_restored(args.directory), 0
    except RestoreError as error:
        return {'ok': False, 'error': str(error), 'report': error.report}, 2


def _add_inputs(parser, root=False):
    # 子命令上的同名选项用 SUPPRESS，避免默认值覆盖写在子命令之前的全局选项。
    default = 'data/source.json' if root else argparse.SUPPRESS
    parser.add_argument('--source', default=default,
                        help='合成资料文档（含 baseline/increments/tables）')
    parser.add_argument('--archive', default=argparse.SUPPRESS,
                        help='全量归档路径')
    parser.add_argument('--log', default=argparse.SUPPRESS,
                        help='增量日志 JSON 路径')
    parser.add_argument('--objects', default=argparse.SUPPRESS,
                        help='按摘要保存的对象目录')
    parser.add_argument('--workdir', default=argparse.SUPPRESS,
                        help='由资料文档构造来源时保留的工作目录')


def main(argv=None):
    parser = argparse.ArgumentParser(description='读取并核对本地资料；按提交位置恢复')
    _add_inputs(parser, root=True)
    common = argparse.ArgumentParser(add_help=False)
    _add_inputs(common)
    commands = parser.add_subparsers(dest='command')

    commands.add_parser('inspect', help='读取并核对现有基础接口',
                        parents=[common])

    plan = commands.add_parser('plan', help='只读核对链并输出恢复计划',
                               parents=[common])
    plan.add_argument('--target', required=True)
    plan.add_argument('--allow-fallback', action='store_true')
    plan.add_argument('--save', help='把计划写入 JSON，供 status/续做使用')
    plan.set_defaults(handler=command_plan)

    recover = commands.add_parser('restore', help='在隔离目录执行恢复',
                                  parents=[common])
    recover.add_argument('--target', required=True)
    recover.add_argument('--directory', required=True)
    recover.add_argument('--allow-fallback', action='store_true')
    recover.set_defaults(handler=command_restore)

    status = commands.add_parser('status', help='查看某计划已完成的阶段')
    status.add_argument('--plan', required=True)
    status.add_argument('--directory', required=True)
    status.set_defaults(handler=command_status)

    opened = commands.add_parser('open', help='打开恢复仓并查询原始行')
    opened.add_argument('--directory', required=True)
    opened.add_argument('--table')
    opened.set_defaults(handler=command_open)

    check = commands.add_parser('verify', help='只凭恢复目录复核')
    check.add_argument('--directory', required=True)
    check.set_defaults(handler=command_verify)

    args = parser.parse_args(argv)
    if args.command is None:
        value, code = command_inspect(args)
        print(json.dumps(value, ensure_ascii=False, sort_keys=True))
        return code
    value, code = args.handler(args)
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))
    return code


if __name__ == '__main__':
    sys.exit(main())

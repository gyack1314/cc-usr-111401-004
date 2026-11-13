"""按指定已提交位置恢复增量备份链。

恢复分两步，彼此绑定：

* `plan_restore` **只读**核对全量基线、增量日志（编号连续、父摘要链接）以及恢复
  目标状态下每张表的对象闭包。目标不可达时抛出 `RestoreError`，`report` 精确
  给出 `recoverable_end`（可恢复的末端提交）和 `blocked`（阻塞阶段/位置/原因）。
* `RestoreRun` 按计划在隔离目录执行恢复，可崩溃后续做。每个阶段先写临时文件、
  `fsync`、原子改名，最后写阶段标记；标记以**计划摘要**命名并再次记录计划摘要，
  因此换一个恢复目标得到的新计划不会沿用旧计划的完成状态。恢复前逐阶段复核标记
  和产物摘要，已经把产物可靠写完但标记未落盘的阶段会被识别为完成而不是重做。

恢复只停在完整提交边界：一笔提交 `changes` 里的所有表一起生效；目标之后的提交
（包括删除表、新增表、修改 schema）完全不参与重建。`changes` 的值为 `None` 表示
删除该表。

来源仓库从不写入：所有复制都落到目标隔离目录，摘要已校验的内容对象可以复用。

校验不是“包存在/复制完/行数相同”：最终会真正打开恢复目录里的 SQLite 仓库，逐表
执行查询，把查询出的原始行与不可变快照逐值比较，并核对字段目录、快照/数据摘要、
表头、引用来源，结果写入 `verification.json` 并返回。
"""
from pathlib import Path
import hashlib
import json
import os
import re
import sqlite3
import tempfile

try:
    import fcntl
except ImportError:  # 非 POSIX 平台：仍可恢复，只是没有进程级互斥。
    fcntl = None

from .objects import ObjectStore, encode
from .backup import create_archive, read_archive
from .snapshot import Snapshots

FORMAT = 1
PLAN_FORMAT = 1
STAGES = ('objects', 'tables', 'manifest', 'verified')
_HEX = re.compile(r'^[0-9a-f]{64}$')
# 表名要同时安全地用作 SQL 标识符和文件名：拒绝路径分隔、控制字符与隐藏名。
_TABLE = re.compile(r'^[^\x00/\\]+$')


class RestoreError(ValueError):
    """恢复无法完成；`report` 是可序列化的裁决报告。"""

    def __init__(self, message, report=None):
        super().__init__(message)
        self.report = report or {}

    def __str__(self):
        base = super().__str__()
        blocked = self.report.get('blocked')
        if not blocked:
            return base
        where = (blocked.get('commit_id') or blocked.get('object')
                 or blocked.get('table') or blocked.get('field'))
        detail = blocked.get('reason')
        text = f'{base}: {detail}' + (f' @ {where}' if where else '')
        end = self.report.get('recoverable_end', blocked.get('recoverable_end'))
        if end is not None or 'recoverable_end' in self.report:
            text += f'（可恢复末端={end}）'
        return text


# 便于调用方按语义捕获。
ChainError = RestoreError
RecoveryError = RestoreError


def is_digest(value):
    return isinstance(value, str) and bool(_HEX.match(value))


def is_table_name(value):
    return (isinstance(value, str) and bool(value) and bool(_TABLE.match(value))
            and value not in ('.', '..') and not value.startswith('.')
            and not value.lower().startswith('sqlite_'))


def digest_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


def digest_value(value):
    return digest_bytes(encode(value))


def _digest_forms(value):
    """同一逻辑值常见的摘要写法：紧凑排序、默认分隔符、ASCII 转义。"""
    variants = [
        json.dumps(value, ensure_ascii=False, sort_keys=True,
                   separators=(',', ':')),
        json.dumps(value, ensure_ascii=False, sort_keys=True),
        json.dumps(value, ensure_ascii=True, sort_keys=True),
        json.dumps(value, ensure_ascii=True, sort_keys=True,
                   separators=(',', ':')),
    ]
    return {digest_bytes(text.encode('utf-8')) for text in variants}


def _read_json(path):
    try:
        return json.loads(Path(path).read_bytes())
    except (OSError, ValueError):
        return None


def _atomic_write(path, raw):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.writing-')
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return digest_bytes(raw)


# ---------------------------------------------------------------------------
# 只读来源


class Source:
    """已验证对象的只读来源。

    `archive_objects` 是全量归档自带的 `{摘要: 字节}`，优先于对象存储中的同名
    摘要。任何 `get` 都重新校验摘要；对象不提供写入接口。
    """

    def __init__(self, store, archive_objects=None):
        self.store = store
        self.archive_objects = dict(archive_objects or {})
        for digest, raw in self.archive_objects.items():
            if not is_digest(digest) or digest_bytes(raw) != digest:
                raise ValueError('归档对象摘要不符')

    @property
    def root(self):
        return getattr(self.store, 'root', None)

    def get(self, digest):
        if not is_digest(digest):
            raise ValueError('对象身份非法')
        if digest in self.archive_objects:
            return self.archive_objects[digest]
        return self.store.get(digest)

    def has(self, digest):
        if not is_digest(digest):
            return False
        try:
            self.get(digest)
        except (OSError, ValueError):
            return False
        return True


def _as_source(source, archive_objects=None):
    inner = source.store if isinstance(source, Source) else source
    merged = dict(getattr(source, 'archive_objects', {}) or {})
    merged.update(archive_objects or {})
    if isinstance(source, Source) and not archive_objects:
        return source
    return Source(inner, merged)


# ---------------------------------------------------------------------------
# 基线


def _baseline_problem(baseline):
    if not isinstance(baseline, dict):
        return '缺少全量基线'
    if not isinstance(baseline.get('backup_id'), str) or not baseline['backup_id']:
        return '基线缺少 backup_id'
    if not isinstance(baseline.get('base_commit'), str) or not baseline['base_commit']:
        return '基线缺少 base_commit'
    heads = baseline.get('table_heads')
    if not isinstance(heads, dict) or not heads:
        return '基线缺少 table_heads'
    if not all(is_table_name(k) for k in heads):
        return '基线表名非法'
    if not all(is_digest(v) for v in heads.values()):
        return '基线表头摘要非法'
    return None


def read_baseline(archive):
    """读取并核对全量归档，返回 `(描述, 归档对象字典)`。

    `archive` 可以是归档路径，或含 `path`/`backup_id`/`base_commit`/`table_heads`
    的描述；没有 `path` 时要求对象已存在于来源存储中。归档成员摘要由
    `read_archive` 校验，这里再核对归档元数据与声明一致、表头都在对象清单中。
    """
    if isinstance(archive, (str, os.PathLike)):
        path, declared = Path(archive), {}
    elif isinstance(archive, dict):
        path = Path(archive['path']) if archive.get('path') else None
        declared = dict(archive)
    else:
        raise RestoreError('基线归档缺失', {'blocked': {
            'stage': 'baseline', 'reason': '未提供全量归档'}})
    if path is None:
        problem = _baseline_problem(declared)
        if problem:
            raise RestoreError('基线不可用', {'blocked': {
                'stage': 'baseline', 'reason': problem}})
        heads = dict(declared['table_heads'])
        body = {'backup_id': declared['backup_id'],
                'base_commit': declared['base_commit'],
                'table_heads': heads}
        return ({'backup_id': declared['backup_id'],
                 'base_commit': declared['base_commit'],
                 'table_heads': heads,
                 'archive_digest': digest_value(body),
                 'archive_path': None}, {})
    try:
        raw_archive = path.read_bytes()
        index, objects = read_archive(path)
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RestoreError('基线归档不可读', {
            'recoverable_end': None, 'blocked': {
                'stage': 'baseline', 'reason': '备份包无法读取',
                'path': str(path), 'detail': str(error)}})
    problem = _baseline_problem(index)
    if problem:
        raise RestoreError('基线归档非法', {'blocked': {
            'stage': 'baseline', 'reason': problem, 'path': str(path)}})
    for key in ('backup_id', 'base_commit', 'table_heads'):
        if key in declared and declared[key] != index.get(key):
            raise RestoreError('基线不匹配', {
                'recoverable_end': None, 'blocked': {
                    'stage': 'baseline', 'reason': '备份包与基线声明不符',
                    'field': key, 'expected': declared[key],
                    'actual': index.get(key), 'path': str(path)}})
    heads = dict(index['table_heads'])
    listed = index.get('objects')
    if not isinstance(listed, list) or not all(is_digest(d) for d in listed):
        raise RestoreError('基线对象清单非法', {'blocked': {
            'stage': 'baseline', 'reason': '备份包对象清单非法',
            'path': str(path)}})
    absent = sorted(set(heads.values()) - set(listed))
    if absent:
        raise RestoreError('基线对象缺失', {'blocked': {
            'stage': 'baseline', 'reason': '基线表头不在归档对象中',
            'missing': absent, 'path': str(path)}})
    for table, head in heads.items():
        try:
            manifest = json.loads(objects[head])
        except (KeyError, ValueError):
            raise RestoreError('基线快照损坏', {'blocked': {
                'stage': 'baseline', 'reason': '基线表头不是合法快照',
                'object': head, 'table': table, 'path': str(path)}})
        data = manifest.get('data') if isinstance(manifest, dict) else None
        if not is_digest(data) or data not in objects:
            raise RestoreError('基线数据缺失', {'blocked': {
                'stage': 'baseline', 'reason': '基线数据对象缺失',
                'object': data, 'table': table, 'path': str(path)}})
    descriptor = {'backup_id': index['backup_id'],
                  'base_commit': index['base_commit'],
                  'table_heads': heads,
                  'archive_digest': digest_bytes(raw_archive),
                  'archive_path': str(path),
                  'index_digest': digest_bytes(encode(index)),
                  'index_digests': sorted(_digest_forms(index)),
                  'archive_digests': sorted({digest_bytes(raw_archive),
                                             digest_bytes(encode(index))})}
    return descriptor, objects


def baseline_candidates(baseline):
    """可接受的日志链起点父摘要。

    规范起点是 `{backup_id, base_commit, table_heads}` 的摘要；同时接受常见的
    归档清单摘要与直接以备份身份/基线提交标识链接的写法，便于读取既有日志。
    只接受本基线自身能推导出的值，因此不会放过错误的基线。
    """
    heads = baseline['table_heads']
    values = [
        {'backup_id': baseline['backup_id'], 'base_commit': baseline['base_commit'],
         'table_heads': heads},
        {'backup_id': baseline['backup_id'], 'base_commit': baseline['base_commit']},
        {'base_commit': baseline['base_commit'], 'table_heads': heads},
        {'format': FORMAT, 'base_commit': baseline['base_commit'],
         'table_heads': heads},
        {'backup_id': baseline['backup_id'], 'base_commit': baseline['base_commit'],
         'table_heads': heads, 'format': FORMAT},
        heads,
    ]
    candidates = set()
    for value in values:
        candidates |= _digest_forms(value)
    candidates.add(baseline.get('archive_digest'))
    candidates.add(baseline.get('index_digest'))
    candidates.update(baseline.get('index_digests') or [])
    candidates.update(baseline.get('archive_digests') or [])
    candidates.update({baseline['backup_id'], baseline['base_commit']})
    return {value for value in candidates if isinstance(value, str)}


def canonical_entry_digest(entry):
    return digest_value({'sequence': entry['sequence'],
                         'commit_id': entry['commit_id'],
                         'changes': entry['changes']})


def entry_candidates(raw, heads_after, heads_before):
    """一条日志项可被后一项引用的摘要形式。"""
    forms = [
        {'sequence': raw['sequence'], 'commit_id': raw['commit_id'],
         'changes': raw['changes']},
        {'commit_id': raw['commit_id'], 'changes': raw['changes']},
        {'commit_id': raw['commit_id'], 'table_heads': heads_after},
        {'commit_id': raw['commit_id'], 'table_heads': heads_before},
        heads_after,
        heads_before,
        raw,
        {key: value for key, value in raw.items() if key != 'parent_digest'},
        {key: value for key, value in raw.items()
         if key not in ('parent_digest', 'digest', 'entry_digest')},
    ]
    candidates = set()
    for value in forms:
        candidates |= _digest_forms(value)
    for key in ('digest', 'entry_digest', 'content_digest', 'log_digest'):
        if is_digest(raw.get(key)):
            candidates.add(raw[key])
    # 兼容直接以父提交身份链接日志的实现。
    candidates.add(raw['commit_id'])
    return candidates


# ---------------------------------------------------------------------------
# 日志链


def _tombstone(value):
    return value is None or (isinstance(value, dict)
                             and (value.get('deleted') is True
                                  or value.get('delete') is True
                                  or value.get('op') == 'delete'))


def _normalise_changes(changes):
    return {table: (None if _tombstone(head) else head)
            for table, head in changes.items()}


def _apply(heads, changes):
    result = dict(heads)
    for table, head in changes.items():
        if _tombstone(head):
            result.pop(table, None)
        else:
            result[table] = head
    return result


def _check_entry(raw, expected_sequence):
    if not isinstance(raw, dict):
        return {'reason': '日志项不是对象'}
    sequence = raw.get('sequence')
    if not isinstance(sequence, int) or isinstance(sequence, bool) \
            or sequence != expected_sequence:
        return {'reason': '日志编号不连续', 'expected': expected_sequence,
                'actual': sequence}
    commit_id = raw.get('commit_id')
    if not isinstance(commit_id, str) or not commit_id:
        return {'reason': '日志项缺少提交身份', 'sequence': sequence}
    changes = raw.get('changes')
    if not isinstance(changes, dict):
        return {'reason': '日志项 changes 不是对象', 'sequence': sequence,
                'commit_id': commit_id}
    for table, head in changes.items():
        if not is_table_name(table):
            return {'reason': '变更表名非法', 'sequence': sequence, 'table': table}
        if _tombstone(head):
            continue
        if not is_digest(head):
            return {'reason': '变更表头摘要非法', 'sequence': sequence,
                    'table': table, 'head': head}
    return None


def _normalise_log(log):
    """接受数组或按编号的映射；按日志编号排序，编号缺口仍会被判为不连续。"""
    if log is None:
        return []
    if isinstance(log, dict):
        items = list(log.values())
    elif isinstance(log, (list, tuple)):
        items = list(log)
    else:
        raise RestoreError('日志格式非法', {'blocked': {
            'stage': 'log', 'reason': '增量日志不是数组或映射'}})
    if items and all(isinstance(item, dict) and isinstance(item.get('sequence'), int)
                     and not isinstance(item.get('sequence'), bool)
                     for item in items):
        items.sort(key=lambda item: item['sequence'])
    return items


def read_chain(target, baseline, log, strict=True):
    """核对日志链并返回 `(可到达目标的完整边界, 报告)`。

    编号必须连续递增；编号连续而父摘要断裂时，`blocked` 指出断裂项，
    `recoverable_end` 给出最后一个可靠提交。目标在可靠前缀内才成功；目标之后
    的日志只出现在 `ignored` 中，不参与恢复。缺少 `parent_digest` 字段的旧日志
    按规范链摘要补全；`strict=False` 时只要求存在正确的前序项，不校验链接值。
    """
    log = _normalise_log(log)
    base_commit = baseline['base_commit']
    expected = baseline_candidates(baseline)
    expected_sequence = 1
    if log and isinstance(log[0], dict) and log[0].get('sequence') == 0:
        expected_sequence = 0  # 兼容从 0 开始编号的日志
    seen, entries, heads = set(), [], dict(baseline['table_heads'])
    blocked = None
    for position, raw in enumerate(log):
        problem = _check_entry(raw, expected_sequence)
        if problem is None and raw['commit_id'] in seen:
            problem = {'reason': '提交身份重复', 'commit_id': raw['commit_id']}
        if problem is None and 'parent_digest' in raw:
            parent = raw['parent_digest']
            if parent not in expected and strict:
                reason = ('基线不匹配：日志起点与基线摘要不符' if position == 0
                          else '父摘要链接断裂')
                problem = {'reason': reason,
                           'commit_id': raw['commit_id'],
                           'expected': sorted(expected)[0] if expected else None,
                           'actual': parent}
        if problem:
            recoverable = entries[-1]['commit_id'] if entries else base_commit
            blocked = dict(problem, stage='log', position=position,
                           after_sequence=expected_sequence - 1,
                           recoverable_end=recoverable)
            break
        before = dict(heads)
        changes = _normalise_changes(raw['changes'])
        heads = _apply(heads, changes)
        entry = {'sequence': raw['sequence'], 'commit_id': raw['commit_id'],
                 'changes': json.loads(encode(changes)),
                 'parent_digest': raw.get('parent_digest'),
                 'entry_digest': canonical_entry_digest(
                     {'sequence': raw['sequence'], 'commit_id': raw['commit_id'],
                      'changes': changes})}
        entries.append(entry)
        seen.add(raw['commit_id'])
        expected = entry_candidates(raw, heads, before)
        expected_sequence += 1
    reachable = [base_commit] + [item['commit_id'] for item in entries]
    final_heads = dict(heads)
    report = {'target': target, 'base_commit': base_commit,
              'reachable': reachable, 'blocked': blocked,
              'recoverable_end': (entries[-1]['commit_id'] if entries
                                  else base_commit),
              'last_recoverable_commit': (entries[-1]['commit_id'] if entries
                                          else base_commit),
              'table_heads': final_heads}
    if target == base_commit:
        if blocked is not None:
            report['later_blocked'] = blocked
            report['blocked'] = None
        return [], report
    position = next((index for index, item in enumerate(entries)
                     if item['commit_id'] == target), None)
    if position is not None:
        if blocked is not None:
            # 目标在断裂之前，恢复点本身有效；断裂只作为后续日志提示。
            report['later_blocked'] = blocked
            report['blocked'] = None
        return entries[:position + 1], report
    if blocked is None:
        report['blocked'] = {'stage': 'target', 'reason': '目标提交不在日志中',
                             'target': target,
                             'recoverable_end': report['recoverable_end'],
                             'last_recoverable_commit': report[
                                 'last_recoverable_commit']}
    raise RestoreError('目标提交不可达', report)


# ---------------------------------------------------------------------------
# 对象闭包


def _snapshot_problem(source, head):
    """读取一个表头；返回 `(快照, None)` 或 `(None, 问题描述)`。"""
    try:
        raw = source.get(head)
    except FileNotFoundError:
        return None, {'reason': '对象缺失', 'object': head}
    except (OSError, ValueError):
        return None, {'reason': '对象损坏', 'object': head}
    try:
        manifest = json.loads(raw)
    except ValueError:
        return None, {'reason': '表头不是合法JSON', 'object': head}
    if not isinstance(manifest, dict) or manifest.get('format') != 1:
        return None, {'reason': '快照格式不支持', 'object': head}
    data = manifest.get('data')
    if not is_digest(data):
        return None, {'reason': '快照数据引用非法', 'object': head}
    try:
        raw_rows = source.get(data)
    except FileNotFoundError:
        return None, {'reason': '数据对象缺失', 'object': data}
    except (OSError, ValueError):
        return None, {'reason': '数据对象损坏', 'object': data}
    try:
        rows = json.loads(raw_rows)
    except ValueError:
        return None, {'reason': '数据对象不是合法JSON', 'object': data}
    if not isinstance(rows, list) or len(rows) != manifest.get('rows'):
        return None, {'reason': '快照行数不符', 'object': head, 'data': data}
    if not all(isinstance(row, dict) and isinstance(row.get('row_id'), str)
               for row in rows):
        return None, {'reason': '行缺少身份', 'object': data}
    identities = [row['row_id'] for row in rows]
    if len(set(identities)) != len(identities):
        return None, {'reason': '行身份重复', 'object': data}
    if not isinstance(manifest.get('schema'), dict):
        return None, {'reason': '字段目录非法', 'object': head}
    return {'snapshot': head, 'data': data, 'rows': rows,
            'schema': manifest['schema']}, None


def inspect_closure(source, table_heads):
    """核对一组表头的对象闭包，返回可读快照与缺失/损坏明细。"""
    source = _as_source(source)
    tables, missing, broken = {}, [], []
    for table in sorted(table_heads):
        head = table_heads[table]
        if head is None:
            continue
        value, problem = _snapshot_problem(source, head)
        if problem:
            broken.append(dict(problem, table=table))
            if problem['reason'].endswith('缺失'):
                missing.append(problem.get('object'))
            continue
        tables[table] = value
    return {'tables': tables, 'missing': sorted({m for m in missing if m}),
            'broken': broken}


def _closure_error(source, heads, stage, recoverable_end, extra=None):
    closure = inspect_closure(source, heads)
    if not closure['broken']:
        return closure, None
    blocked = {'stage': stage, 'reason': '对象闭包不完整',
               'missing': closure['missing'], 'broken': closure['broken'],
               'recoverable_end': recoverable_end}
    if extra:
        blocked.update(extra)
    return closure, blocked


# ---------------------------------------------------------------------------
# 规划


def _plan_steps(source, target, baseline, log, report):
    """核对可达性并逐提交检查闭包；结果写入 report，失败不抛异常。"""
    report['blocked'] = None
    try:
        steps, chain_report = read_chain(target, baseline, log)
    except RestoreError as error:
        report.update({key: error.report[key] for key in
                       ('reachable', 'recoverable_end', 'blocked')
                       if key in error.report})
        report['blocked'] = error.report.get('blocked')
        report['recoverable_end'] = error.report.get('recoverable_end')
        return [], {}
    report.update({key: chain_report[key] for key in
                   ('reachable', 'recoverable_end', 'blocked')})
    if 'later_blocked' in chain_report:
        report['later_blocked'] = chain_report['later_blocked']
    heads = dict(baseline['table_heads'])
    preceding = baseline['base_commit']
    _, blocked = _closure_error(source, heads, 'baseline', None)
    if blocked:
        blocked['recoverable_end'] = None
        report['blocked'] = blocked
        report['recoverable_end'] = None
        report['table_heads'] = heads
        return [], chain_report
    for item in steps:
        changed = {table: head for table, head in item['changes'].items()
                   if head is not None}
        _, blocked = _closure_error(
            source, changed, 'commit', preceding,
            {'sequence': item['sequence'], 'commit_id': item['commit_id']})
        if blocked:
            report['blocked'] = blocked
            report['recoverable_end'] = preceding
            report['last_recoverable_commit'] = preceding
            report['table_heads'] = heads
            return [], chain_report
        heads = _apply(heads, item['changes'])
        preceding = item['commit_id']
    _, blocked = _closure_error(source, heads, 'target', preceding)
    if blocked:
        report['blocked'] = blocked
        report['recoverable_end'] = preceding
        report['last_recoverable_commit'] = preceding
        report['table_heads'] = heads
        return [], chain_report
    report['table_heads'] = heads
    report['recoverable_end'] = preceding
    return steps, chain_report


def plan_restore(source=None, target=None, archive=None, log=None,
                 allow_fallback=False, **kwargs):
    """只读核对并返回恢复计划；不可恢复时抛出 `RestoreError`。

    `source` 为 `ObjectStore` 或 `Source`；`archive` 为全量归档路径/描述；
    `log` 为增量日志数组（或按 sequence 编号的映射）。返回的计划可 JSON 序列化，
    `plan_digest` 绑定基线、目标、步骤、表头、闭包与日志末端。

    `allow_fallback=True` 时，若目标不可达则退到报告的 `recoverable_end`，并在
    计划里记录 `requested_target`。默认不降级，避免悄悄恢复到错误位置。
    """
    source = _resolve_alias(kwargs, ('store', 'objects', 'object_store', 'data'),
                            source)
    target = _resolve_alias(kwargs, ('commit', 'commit_id', 'restore_to',
                                     'target_commit'), target)
    archive = _resolve_alias(kwargs, ('baseline', 'baseline_archive', 'full',
                                      'archive_path', 'base'), archive)
    log = _resolve_alias(kwargs, ('increments', 'entries', 'journal', 'logs',
                                  'increment_log'), log)
    allow_fallback = kwargs.pop('fallback', allow_fallback)
    workdir = kwargs.pop('workdir', None)
    if kwargs:
        raise TypeError(f'未知参数：{sorted(kwargs)}')
    if isinstance(source, dict) and 'baseline' in source and archive is None:
        # 直接给合成资料文档：临时构造来源后再规划。
        if workdir is None:
            raise TypeError('以资料文档规划需要 workdir，或改用 restore_document')
        built = build_source(source, workdir)
        source, archive, log = built['store'], built['archive'], built['log']
    if source is None or target is None or archive is None:
        raise TypeError('plan_restore 需要 source/target/archive')
    if not isinstance(target, str) or not target:
        raise RestoreError('恢复目标非法', {'target': target, 'blocked': {
            'stage': 'plan', 'reason': '目标提交为空'}})
    baseline, archive_objects = read_baseline(archive)
    source = _as_source(source, archive_objects)
    report = {'target': target, 'baseline': {
        'backup_id': baseline['backup_id'],
        'base_commit': baseline['base_commit'],
        'table_heads': baseline['table_heads'],
        'archive_digest': baseline['archive_digest']}}
    requested = target
    applied_so_far = set()
    log = log or []
    steps, chain_report = _plan_steps(source, target, baseline, log, report)
    if allow_fallback:
        # 目标不可达/闭包不完整时，退到最后可完整物化的提交边界。
        while True:
            failure = report.get('blocked')
            if failure is None:
                break
            fallback = report.get('recoverable_end')
            if not isinstance(fallback, str) or fallback == target \
                    or fallback in (report.get('requested_target'), *applied_so_far):
                raise RestoreError('目标提交不可达', report)
            applied_so_far.add(target)
            target = fallback
            report['requested_target'] = requested
            report['fallback'] = True
            steps, chain_report = _plan_steps(source, target, baseline, log,
                                              report)
    elif report.get('blocked') is not None:
        # 保留首个失败点：计划闭包不完整时也应说明末端提交。
        raise RestoreError('目标提交不可达', report)
    report.update({key: chain_report[key] for key in
                   ('reachable', 'recoverable_end', 'blocked')})

    heads = dict(baseline['table_heads'])
    for item in steps:
        heads = _apply(heads, item['changes'])
    closure = inspect_closure(source, heads)

    applied = [item['commit_id'] for item in steps]
    report['applied'] = applied
    report['ignored'] = [raw.get('commit_id') for raw in _normalise_log(log)
                         if isinstance(raw, dict)
                         and raw.get('commit_id') not in set(applied)]
    report['recoverable_end'] = target
    report['last_recoverable_commit'] = target
    plan = {
        'format': PLAN_FORMAT,
        'target': target,
        'baseline': baseline,
        'steps': steps,
        'log_head': steps[-1]['entry_digest'] if steps else digest_value({
            'backup_id': baseline['backup_id'],
            'base_commit': baseline['base_commit'],
            'table_heads': baseline['table_heads']}),
        'table_heads': heads,
        'tables': {
            table: {'snapshot': value['snapshot'], 'data': value['data'],
                    'schema': value['schema'], 'rows': len(value['rows']),
                    'row_ids': [row['row_id'] for row in value['rows']]}
            for table, value in sorted(closure['tables'].items())},
    }
    if 'requested_target' in report:
        plan['requested_target'] = report['requested_target']
    plan['plan_digest'] = digest_value(plan_body(plan))
    report['plan_digest'] = plan['plan_digest']
    plan['report'] = report
    return plan


def plan_body(plan):
    """计划参与摘要的规范化内容。

    归档路径是运行位置、report 是核对过程的裁决记录，二者都不构成计划身份。
    """
    body = {key: value for key, value in plan.items()
            if key not in ('plan_digest', 'report')}
    baseline = body.get('baseline')
    if isinstance(baseline, dict):
        body['baseline'] = {key: value for key, value in baseline.items()
                            if key != 'archive_path'}
    return body


def _check_plan(plan):
    if not isinstance(plan, dict) or plan.get('format') != PLAN_FORMAT:
        raise RestoreError('恢复计划非法', {'blocked': {
            'stage': 'plan', 'reason': '计划格式不支持'}})
    body = plan_body(plan)
    # 执行端只依赖计划正文，不接受夹带的报告/路径以外的额外字段。
    allowed = {'format', 'target', 'baseline', 'steps', 'log_head',
               'table_heads', 'tables', 'plan_digest', 'requested_target',
               'report'}
    if set(plan) - allowed:
        raise RestoreError('恢复计划含未知字段', {'blocked': {
            'stage': 'plan', 'reason': '计划字段非法',
            'fields': sorted(set(plan) - allowed)}})
    if digest_value(body) != plan.get('plan_digest'):
        raise RestoreError('恢复计划被修改', {'blocked': {
            'stage': 'plan', 'reason': '计划摘要不符'}})
    if not isinstance(plan.get('baseline'), dict) \
            or not isinstance(plan.get('steps'), list) \
            or not isinstance(plan.get('tables'), dict):
        raise RestoreError('恢复计划字段缺失', {'blocked': {
            'stage': 'plan', 'reason': '计划结构不完整'}})
    computed = _apply(plan['baseline']['table_heads'],
                      {key: value for step in plan['steps']
                       for key, value in step['changes'].items()})
    if computed != plan['table_heads']:
        raise RestoreError('恢复计划表头不符', {'blocked': {
            'stage': 'plan', 'reason': '步骤与目标表头不一致'}})
    if not all(is_table_name(table) for table in plan['table_heads']):
        raise RestoreError('恢复计划表名非法', {'blocked': {
            'stage': 'plan', 'reason': '表名不能用作文件与SQL标识符'}})
    if set(plan['tables']) != set(plan['table_heads']):
        raise RestoreError('恢复计划表集合不符', {'blocked': {
            'stage': 'plan', 'reason': '表内容与表头不一致'}})
    if any(info.get('snapshot') != plan['table_heads'][table]
           for table, info in plan['tables'].items()):
        raise RestoreError('恢复计划表头不符', {'blocked': {
            'stage': 'plan', 'reason': '表内容的快照与表头不一致'}})
    if plan['target'] != plan['baseline']['base_commit'] \
            and not any(step['commit_id'] == plan['target']
                        for step in plan['steps']):
        raise RestoreError('恢复计划未停在目标边界', {'blocked': {
            'stage': 'plan', 'reason': '目标不在提交边界'}})


# ---------------------------------------------------------------------------
# 阶段状态与续做


def _state_root(directory):
    return Path(directory) / '.restore-state'


def save_plan(plan, path):
    """把恢复计划写成可复核的 JSON；写盘后调用方可用 status 查看进度。"""
    _check_plan(plan)
    _atomic_write(path, encode(plan))
    return plan['plan_digest']


def load_plan(path):
    """读取并校验计划摘要；被修改过的计划拒绝执行。"""
    plan = _read_json(path)
    if not isinstance(plan, dict):
        raise RestoreError('恢复计划无法读取', {'blocked': {
            'stage': 'plan', 'reason': '计划不是合法JSON', 'path': str(path)}})
    _check_plan(plan)
    return plan


def _stage_path(directory, plan_digest, stage):
    return _state_root(directory) / plan_digest / f'{stage}.json'


def _write_marker(directory, plan_digest, stage, artifacts):
    value = {'plan_digest': plan_digest, 'stage': stage, 'artifacts': artifacts}
    _atomic_write(_stage_path(directory, plan_digest, stage), encode(value))
    return value


def _artifact_digest(directory, relative):
    path = Path(directory) / relative
    try:
        return digest_bytes(path.read_bytes())
    except OSError:
        return None


def _marker_status(directory, plan_digest, stage):
    marker = _read_json(_stage_path(directory, plan_digest, stage))
    if not isinstance(marker, dict) or marker.get('plan_digest') != plan_digest \
            or marker.get('stage') != stage \
            or not isinstance(marker.get('artifacts'), dict):
        return False, '缺少该计划的有效阶段标记'
    for relative, expected in marker['artifacts'].items():
        parts = Path(relative).parts
        if Path(relative).is_absolute() or '..' in parts:
            return False, '阶段标记路径非法'
        actual = _artifact_digest(directory, relative)
        if actual is None or actual != expected:
            return False, f'阶段产物已损坏：{relative}'
    return True, '标记与产物一致'


def stage_status(directory, plan_digest):
    """报告某计划各阶段是否可靠完成（检查标记和目标产物摘要）。

    标记以计划摘要命名，因此换目标后另一份计划的完成标记不会出现在这里。
    """
    status = {}
    for stage in STAGES:
        complete, reason = _marker_status(directory, plan_digest, stage)
        status[stage] = {'complete': complete, 'reason': reason}
    return status


def completed_stages(directory, plan_digest):
    """返回该计划已可靠完成的阶段（按恢复顺序）。"""
    return [stage for stage in STAGES
            if _marker_status(directory, plan_digest, stage)[0]]


def resume_status(directory, plan_digest):
    """`stage_status` 的别名，便于运维读取续做状态。"""
    return stage_status(directory, plan_digest)


# ---------------------------------------------------------------------------
# SQLite 仓库


_TYPE_MAP = {
    'int': 'int', 'integer': 'int', 'bigint': 'int', 'smallint': 'int',
    'bool': 'bool', 'boolean': 'bool',
    'float': 'float', 'real': 'float', 'double': 'float', 'decimal': 'float',
    'numeric': 'float', 'number': 'float',
    'blob': 'blob', 'bytes': 'blob', 'binary': 'blob',
    'text': 'text', 'string': 'text', 'str': 'text', 'varchar': 'text',
    'char': 'text', 'uuid': 'text', 'date': 'text', 'datetime': 'text',
    'timestamp': 'text', 'json': 'json', 'jsonb': 'json', 'object': 'json',
    'array': 'json', 'list': 'json',
}


def _quote(name):
    if not is_table_name(name):
        raise ValueError(f'表名非法：{name!r}')
    return '"' + name.replace('"', '""') + '"'


def _sample_kind(sample):
    if isinstance(sample, bool):
        return 'bool'
    if isinstance(sample, int):
        return 'int'
    if isinstance(sample, float):
        return 'float'
    if isinstance(sample, bytes):
        return 'blob'
    if isinstance(sample, (list, dict)):
        return 'json'
    return 'text'


def _column_kind(declared, sample):
    text = declared.strip().lower() if isinstance(declared, str) else ''
    fallback = _sample_kind(sample)
    kind = _TYPE_MAP.get(text)
    if kind is None or sample is None:
        return kind or fallback
    compatible = {
        'int': isinstance(sample, int) and not isinstance(sample, bool),
        'bool': isinstance(sample, (bool, int)) and not isinstance(sample, float),
        'float': isinstance(sample, (int, float)) and not isinstance(sample, bool),
        'text': isinstance(sample, str),
        'blob': isinstance(sample, bytes),
        'json': isinstance(sample, (list, dict)),
    }
    return kind if compatible.get(kind, True) else fallback


def _sql_type(kind):
    return {'int': 'INTEGER', 'bool': 'INTEGER', 'float': 'REAL',
            'blob': 'BLOB', 'json': 'TEXT', 'text': 'TEXT'}[kind]


def _encode_cell(kind, value):
    if value is None:
        return None
    if kind == 'bool':
        return 1 if value else 0
    if kind == 'json':
        return encode(value)
    if kind in ('int', 'float', 'text', 'blob'):
        return value
    raise ValueError('列类型不支持')


def _decode_cell(kind, value):
    if value is None:
        return None
    if kind == 'bool':
        return bool(value)
    if kind == 'json':
        return json.loads(value)
    return value


def _table_layout(schema, rows):
    """列目录：schema 字段在前，行里额外字段（如 row_id）按名字补在后面。"""
    names = list(schema)
    extras = sorted({key for row in rows for key in row} - set(names))
    fields = []
    for name in names + extras:
        if not isinstance(name, str) or not name or '\x00' in name:
            raise ValueError(f'字段名非法：{name!r}')
        sample = next((row[name] for row in rows if row.get(name) is not None), None)
        kind = _column_kind(schema.get(name), sample)
        fields.append({'name': name, 'declared': schema.get(name), 'kind': kind,
                       'sql_type': _sql_type(kind)})
    return fields


def _build_database(path, snapshots):
    """从快照原子构建 SQLite 仓库；返回每张表的列目录。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.building-', suffix='.db')
    os.close(fd)
    layouts = {}
    try:
        connection = sqlite3.connect(name)
        try:
            connection.execute('PRAGMA journal_mode=DELETE')
            for table, snapshot in sorted(snapshots.items()):
                fields = _table_layout(snapshot['schema'], snapshot['rows'])
                layouts[table] = fields
                if fields:
                    columns = ', '.join(
                        f'{_quote(field["name"])} {field["sql_type"]}'
                        for field in fields)
                else:
                    # SQLite 不支持零列表；用占位列承载"空字段目录"的表。
                    columns = '"_placeholder" INTEGER'
                connection.execute(f'CREATE TABLE {_quote(table)} ({columns})')
                if fields:
                    placeholders = ', '.join('?' for _ in fields)
                    statement = (f'INSERT INTO {_quote(table)} ('
                                 + ', '.join(_quote(field['name']) for field in fields)
                                 + f') VALUES ({placeholders})')
                    values = [[_encode_cell(field['kind'], row.get(field['name']))
                               for field in fields] for row in snapshot['rows']]
                    connection.executemany(statement, values)
            connection.commit()
        finally:
            connection.close()
        with open(name, 'rb') as stream:
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return layouts


def _project_rows(rows, names):
    """按列目录投影快照行；缺失字段视为 NULL，与数据库查询结果可直接比较。"""
    return [{name: row.get(name) for name in names} for row in rows]


def _query_database(path, layouts=None):
    """真正打开仓库查询所有行；返回 `{表: {'columns', 'fields', 'rows'}}`。"""
    connection = sqlite3.connect(f'file:{Path(path)}?mode=ro', uri=True)
    try:
        names = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        result = {}
        for name in names:
            info = list(connection.execute(f'PRAGMA table_info({_quote(name)})'))
            fields = (layouts or {}).get(name)
            columns = [row[1] for row in info]
            if fields is not None and not fields:
                columns = []  # 零列表用占位列承载，不是字段目录的一部分
            kinds = {field['name']: field['kind'] for field in (fields or [])}
            order = ' ORDER BY "row_id"' if 'row_id' in columns else ''
            projection = (', '.join(_quote(column) for column in columns)
                          if columns else 'NULL')
            rows = []
            for raw in connection.execute(
                    f'SELECT {projection} FROM {_quote(name)}{order}'):
                rows.append({column: _decode_cell(kinds.get(column, 'text'), value)
                             for column, value in zip(columns, raw)})
            result[name] = {'columns': columns, 'fields': fields, 'rows': rows}
        return result
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# 执行


class RestoreRun:
    """在隔离目录执行一份恢复计划；可重复调用以续做。"""

    def __init__(self, plan, source, directory):
        _check_plan(plan)
        self.plan = plan
        # 基线对象可能只存在于全量归档里；只读打开以补齐来源，不写任何位置。
        archive_objects = {}
        path = plan['baseline'].get('archive_path')
        if path:
            try:
                _, archive_objects = read_archive(path)
            except (OSError, ValueError, KeyError, TypeError):
                archive_objects = {}
        self.source = _as_source(source, archive_objects)
        self.directory = Path(directory)
        self.store = ObjectStore(self.directory / 'objects')

    # -- 安全 ---------------------------------------------------------------

    def _assert_isolated(self):
        source_root = self.source.root
        if source_root is None:
            return
        source_root = Path(source_root).resolve()
        destination = self.directory.resolve()
        if destination == source_root or source_root in destination.parents:
            raise RestoreError('恢复目录不能位于来源仓库内', {'blocked': {
                'stage': 'plan', 'reason': '会写入正在提供查询的仓库',
                'source': str(source_root), 'destination': str(destination)}})

    def _clear_temporary_files(self):
        """清理上次崩溃留下的临时文件；只删除本模块命名的工作文件。"""
        patterns = ('.writing-*', '.staging-*', '.building-*', '.table-*',
                    '.marker-*')
        for pattern in patterns:
            for path in self.directory.rglob(pattern):
                try:
                    if path.is_file():
                        path.unlink()
                except OSError:
                    pass
            for path in (self.store.root).glob(pattern):
                try:
                    if path.is_file():
                        path.unlink()
                except OSError:
                    pass

    # -- 计划推导 -----------------------------------------------------------

    def _required(self):
        required = {}

        def add(head, table, origin, **extra):
            if not is_digest(head):
                raise RestoreError('对象身份非法', {'blocked': {
                    'stage': 'objects', 'object': head, 'reason': '表头摘要非法',
                    'table': table, **extra}})
            meta = {'table': table, 'origin': origin, **extra}
            required.setdefault(head, meta)

        for table, head in self.plan['baseline']['table_heads'].items():
            add(head, table, 'baseline', commit_id=self.plan['baseline']['base_commit'])
        for step in self.plan['steps']:
            for table, head in step['changes'].items():
                if head is not None:
                    add(head, table, 'commit', commit_id=step['commit_id'],
                        sequence=step['sequence'])
        for table, info in self.plan['tables'].items():
            add(info['snapshot'], table, 'target', commit_id=self.plan['target'])
        resolved = dict(required)
        planned = {info['snapshot']: info for info in self.plan['tables'].values()}
        for head, meta in list(required.items()):
            if head in planned and is_digest(planned[head].get('data')):
                resolved.setdefault(planned[head]['data'], {
                    'table': meta.get('table'), 'origin': 'data', 'object':
                    planned[head]['data']})
                continue
            # 续做时目标目录里可能已有对象；优先本地，只有本地缺失才读来源。
            try:
                manifest = json.loads(self.store.get(head))
            except (OSError, ValueError):
                try:
                    manifest = json.loads(self.source.get(head))
                except FileNotFoundError:
                    raise RestoreError('对象缺失', {'blocked': {
                        'stage': 'objects', 'object': head, 'reason': '快照对象缺失',
                        **meta}})
                except (OSError, ValueError) as error:
                    raise RestoreError('对象不可读', {'blocked': {
                        'stage': 'objects', 'object': head, 'reason': '快照对象损坏',
                        'detail': str(error), **meta}})
            data = manifest.get('data') if isinstance(manifest, dict) else None
            if not is_digest(data):
                raise RestoreError('对象引用非法', {'blocked': {
                    'stage': 'objects', 'object': head, 'reason': '快照数据引用非法',
                    **meta}})
            resolved.setdefault(data, {'table': meta.get('table'), 'origin': 'data',
                                       'object': data})
        return resolved

    # -- 阶段识别 -----------------------------------------------------------

    def stage_complete(self, stage):
        """判断阶段是否可靠完成：标记优先，标记缺失时用产物内容识别。"""
        marked, reason = _marker_status(self.directory, self.plan['plan_digest'], stage)
        if marked:
            return True, reason
        if stage == 'objects':
            ok = self._objects_complete()
            return ok, ('产物完整，补写标记' if ok else reason)
        if stage == 'tables':
            ok, why = self._tables_complete()
            return ok, ('产物完整，补写标记' if ok else why)
        if stage == 'manifest':
            ok, why = self._manifest_complete()
            return ok, ('产物完整，补写标记' if ok else why)
        ok = self._verification_complete()
        return ok, ('产物完整，补写标记' if ok else reason)

    # -- 阶段 1：对象 -------------------------------------------------------

    def _objects_complete(self):
        try:
            required = self._required()
        except RestoreError:
            return False
        for digest in required:
            try:
                self.store.get(digest)
            except (OSError, ValueError):
                return False
        return True

    def _objects_artifacts(self):
        return {f'objects/{digest}': digest for digest in self._required()}

    def stage_objects(self):
        """复制计划闭包内的内容对象；目标已有的同摘要对象复核后复用。"""
        required = self._required()
        copied = 0
        for digest in sorted(required):
            try:
                self.store.get(digest)
                continue
            except (OSError, ValueError):
                pass
            try:
                raw = self.source.get(digest)
            except FileNotFoundError:
                raise RestoreError('对象缺失', {'blocked': {
                    'stage': 'objects', 'object': digest, 'reason': '来源对象缺失',
                    **required[digest]}})
            except (OSError, ValueError):
                raise RestoreError('对象损坏', {'blocked': {
                    'stage': 'objects', 'object': digest, 'reason': '来源对象损坏',
                    **required[digest]}})
            if digest_bytes(raw) != digest:
                raise RestoreError('对象摘要不符', {'blocked': {
                    'stage': 'objects', 'object': digest, 'reason': '来源对象摘要不符',
                    **required[digest]}})
            _atomic_write(self.store.root / digest, raw)
            if self.store.get(digest) != raw:
                raise RestoreError('对象复制失败', {'blocked': {
                    'stage': 'objects', 'object': digest, 'reason': '复制后校验失败',
                    **required[digest]}})
            copied += 1
        return _write_marker(self.directory, self.plan['plan_digest'], 'objects',
                             self._objects_artifacts()), copied

    # -- 阶段 2：表 ---------------------------------------------------------

    def _target_snapshots(self):
        snapshots = {}
        for table, head in self.plan['table_heads'].items():
            value = Snapshots(self.store).read(head)
            planned = self.plan['tables'][table]
            if planned.get('snapshot') != head:
                raise RestoreError('表头与计划不符', {'blocked': {
                    'stage': 'tables', 'table': table, 'reason': '快照摘要不符'}})
            if [row['row_id'] for row in value['rows']] != planned.get('row_ids'):
                raise RestoreError('表内容与计划不符', {'blocked': {
                    'stage': 'tables', 'table': table, 'reason': '行身份与计划不符'}})
            snapshots[table] = value
        return snapshots

    def _tables_artifacts(self):
        artifacts = {}
        for path in sorted((self.directory / 'tables').glob('*.json')):
            artifacts[f'tables/{path.name}'] = digest_bytes(path.read_bytes())
        for name in ('catalog.json', 'refs.json', 'warehouse.sqlite3'):
            digest = _artifact_digest(self.directory, name)
            if digest is not None:
                artifacts[name] = digest
        return artifacts

    def _tables_complete(self):
        catalog = _read_json(self.directory / 'catalog.json')
        if not isinstance(catalog, dict) \
                or catalog.get('plan_digest') != self.plan['plan_digest'] \
                or catalog.get('target') != self.plan['target']:
            return False, '目录不属于该计划'
        try:
            snapshots = self._target_snapshots()
        except (RestoreError, ValueError, KeyError, OSError):
            return False, '目标快照无法读取'
        expected = {table: encode({'schema': value['schema'],
                                   'rows': value['rows']})
                    for table, value in snapshots.items()}
        files = sorted((self.directory / 'tables').glob('*.json'))
        if {path.stem for path in files} != set(expected):
            return False, '表文件集合不符'
        for path in files:
            try:
                if path.read_bytes() != expected[path.stem]:
                    return False, f'表文件内容不符：{path.name}'
            except OSError:
                return False, f'表文件不可读：{path.name}'
        database = self.directory / 'warehouse.sqlite3'
        if not database.is_file():
            return False, '仓库数据库缺失'
        try:
            queried = _query_database(database, catalog.get('fields'))
        except (sqlite3.Error, OSError, ValueError):
            return False, '仓库数据库无法打开'
        if set(queried) != set(expected):
            return False, '仓库表集合不符'
        recorded = catalog.get('fields', {})
        if set(recorded) != set(expected):
            return False, '目录字段记录集合不符'
        for table, value in snapshots.items():
            fields = recorded.get(table, [])
            if [field['name'] for field in fields][:len(value['schema'])] \
                    != list(value['schema']):
                return False, f'目录字段目录与schema不符：{table}'
            names = [field['name'] for field in fields]
            expected_rows = sorted(_project_rows(value['rows'], names),
                                   key=lambda row: row['row_id'])
            if queried[table]['rows'] != expected_rows:
                return False, f'仓库行内容不符：{table}'
            if sorted(queried[table]['columns']) != sorted(names):
                return False, f'仓库字段目录不符：{table}'
        return True, '已完成'

    def materialize(self):
        """按目标完整提交边界重建表、目录和可打开的仓库。"""
        snapshots = self._target_snapshots()
        tables_root = self.directory / 'tables'
        tables_root.mkdir(parents=True, exist_ok=True)
        for path in tables_root.glob('*.json'):
            if path.stem not in snapshots:
                path.unlink()
        for table, value in sorted(snapshots.items()):
            _atomic_write(tables_root / f'{table}.json',
                          encode({'schema': value['schema'], 'rows': value['rows']}))
        layouts = _build_database(self.directory / 'warehouse.sqlite3', snapshots)
        # 重建后立刻真正打开仓库查询，阶段产物自身先过一遍内容校验。
        queried = _query_database(self.directory / 'warehouse.sqlite3', layouts)
        if set(queried) != set(snapshots):
            raise RestoreError('仓库表集合不符', {'blocked': {
                'stage': 'tables', 'reason': '重建后表集合不符'}})
        for table, value in snapshots.items():
            names = [field['name'] for field in layouts[table]]
            if queried[table]['rows'] != sorted(
                    _project_rows(value['rows'], names),
                    key=lambda row: row['row_id']):
                raise RestoreError('仓库行内容不符', {'blocked': {
                    'stage': 'tables', 'table': table, 'reason': '重建后行内容不符'}})
        catalog = {
            'format': FORMAT, 'plan_digest': self.plan['plan_digest'],
            'target': self.plan['target'],
            'tables': {table: {
                'schema': value['schema'],
                'fields': layouts[table],
                'snapshot': self.plan['table_heads'][table],
                'data': self.plan['tables'][table]['data'],
                'rows': len(value['rows']),
                'row_ids': [row['row_id'] for row in value['rows']],
            } for table, value in sorted(snapshots.items())},
            'fields': layouts,
        }
        _atomic_write(self.directory / 'catalog.json', encode(catalog))
        refs = {table: self.plan['table_heads'][table] for table in snapshots}
        _atomic_write(self.directory / 'refs.json', encode(refs))
        return _write_marker(self.directory, self.plan['plan_digest'], 'tables',
                             self._tables_artifacts())

    # -- 阶段 3：清单 -------------------------------------------------------

    def _manifest_value(self):
        closure, objects = {}, set()
        for table, info in sorted(self.plan['tables'].items()):
            head = info['snapshot']
            manifest = json.loads(self.store.get(head))
            data = manifest['data']
            raw_rows = self.store.get(data)
            rows = json.loads(raw_rows)
            objects.update((head, data))
            closure[table] = {
                'snapshot': head,
                'schema': manifest['schema'],
                'schema_digest': digest_value(manifest['schema']),
                'data': data,
                'data_digest': digest_bytes(raw_rows),
                'rows': len(rows),
                'row_ids': sorted(row['row_id'] for row in rows),
            }
        for head in self.plan['baseline']['table_heads'].values():
            objects.add(head)
            objects.add(json.loads(self.store.get(head))['data'])
        for step in self.plan['steps']:
            for head in step['changes'].values():
                if head is not None:
                    objects.add(head)
                    objects.add(json.loads(self.store.get(head))['data'])
        return {
            'format': FORMAT,
            'plan_digest': self.plan['plan_digest'],
            'target': self.plan['target'],
            'baseline': {
                'backup_id': self.plan['baseline']['backup_id'],
                'base_commit': self.plan['baseline']['base_commit'],
                'archive_digest': self.plan['baseline'].get('archive_digest'),
                'archive_path': self.plan['baseline'].get('archive_path'),
                'table_heads': self.plan['baseline']['table_heads'],
            },
            'commits': [self.plan['baseline']['base_commit']]
                       + [step['commit_id'] for step in self.plan['steps']],
            'log_head': self.plan['log_head'],
            'table_heads': self.plan['table_heads'],
            'closure': closure,
            'objects': sorted(objects),
        }

    def _manifest_complete(self):
        manifest = _read_json(self.directory / 'manifest.json')
        if not isinstance(manifest, dict):
            return False, '清单缺失'
        if manifest.get('plan_digest') != self.plan['plan_digest']:
            return False, '清单不属于该计划'
        try:
            expected = self._manifest_value()
        except (RestoreError, ValueError, KeyError, OSError) as error:
            return False, f'清单引用对象不可读：{error}'
        if manifest != expected:
            differing = sorted(key for key in set(manifest) | set(expected)
                               if manifest.get(key) != expected.get(key))
            return False, '清单内容与计划不符：' + ', '.join(differing)
        index = _read_json(self.directory / 'index.json')
        want = {'format': FORMAT, 'plan_digest': self.plan['plan_digest'],
                'target': self.plan['target'],
                'baseline': expected['baseline'],
                'table_heads': expected['table_heads'],
                'objects': expected['objects']}
        if index != want:
            return False, '清单索引与计划不符'
        return True, '已完成'

    def write_manifest(self):
        value = self._manifest_value()
        _atomic_write(self.directory / 'manifest.json', encode(value))
        index = {'format': FORMAT, 'plan_digest': self.plan['plan_digest'],
                 'target': self.plan['target'],
                 'baseline': value['baseline'],
                 'table_heads': value['table_heads'],
                 'objects': value['objects']}
        _atomic_write(self.directory / 'index.json', encode(index))
        return _write_marker(self.directory, self.plan['plan_digest'], 'manifest', {
            'manifest.json': _artifact_digest(self.directory, 'manifest.json'),
            'index.json': _artifact_digest(self.directory, 'index.json'),
        })

    # -- 阶段 4：校验 -------------------------------------------------------

    def _verification_complete(self):
        verification = _read_json(self.directory / 'verification.json')
        if not isinstance(verification, dict):
            return False
        if verification.get('plan_digest') != self.plan['plan_digest']:
            return False
        return verification.get('database_digest') == _artifact_digest(
            self.directory, 'warehouse.sqlite3')

    def verify(self):
        """重新打开恢复仓、逐表查询原始行，并核对全部校验材料。"""
        manifest = _read_json(self.directory / 'manifest.json')
        catalog = _read_json(self.directory / 'catalog.json')
        refs = _read_json(self.directory / 'refs.json')
        if not isinstance(manifest, dict) or not isinstance(catalog, dict) \
                or not isinstance(refs, dict):
            raise RestoreError('恢复仓不完整', {'target': self.plan['target'],
                                                'blocked': {
                                                    'stage': 'verify',
                                                    'reason': '清单/目录/引用缺失'}})
        if manifest.get('plan_digest') != self.plan['plan_digest'] \
                or catalog.get('plan_digest') != self.plan['plan_digest']:
            raise RestoreError('恢复仓属于另一份计划', {'target': self.plan['target'],
                                                        'blocked': {
                                                            'stage': 'verify',
                                                            'reason': '计划摘要不符'}})
        expected_manifest = self._manifest_value()
        if manifest != expected_manifest:
            differing = sorted(key for key in set(manifest) | set(expected_manifest)
                               if manifest.get(key) != expected_manifest.get(key))
            raise RestoreError('清单与计划不符', {'blocked': {
                'stage': 'verify', 'reason': '清单内容与计划不符',
                'fields': differing}})
        try:
            queried = _query_database(self.directory / 'warehouse.sqlite3',
                                      catalog.get('fields'))
        except (sqlite3.Error, OSError) as error:
            raise RestoreError('恢复仓无法打开', {'blocked': {
                'stage': 'verify', 'reason': '数据库无法打开',
                'detail': str(error)}})
        problems, checks, result_rows = [], [], {}
        expected_tables = set(self.plan['table_heads'])
        if set(queried) != expected_tables:
            problems.append({'reason': '仓库表集合不符',
                             'expected': sorted(expected_tables),
                             'actual': sorted(queried)})
        if refs != {table: self.plan['table_heads'][table]
                    for table in expected_tables}:
            problems.append({'reason': '目录引用与计划表头不符'})
        files = {path.stem for path in (self.directory / 'tables').glob('*.json')}
        if files != expected_tables:
            problems.append({'reason': '表文件集合不符',
                             'expected': sorted(expected_tables),
                             'actual': sorted(files)})
        for table in sorted(expected_tables):
            reference = self.plan['tables'].get(table)
            if reference is None:
                problems.append({'reason': '计划缺少表', 'table': table})
                continue
            snapshot = Snapshots(self.store).read(reference['snapshot'])
            raw_rows = snapshot['rows']
            table_file = _read_json(self.directory / 'tables' / f'{table}.json')
            if table_file != {'schema': snapshot['schema'], 'rows': raw_rows}:
                problems.append({'reason': '表文件与快照不符', 'table': table})
            if [row['row_id'] for row in raw_rows] != reference.get('row_ids'):
                problems.append({'reason': '行身份与计划不符', 'table': table})
            if table not in queried:
                problems.append({'reason': '仓库缺少表', 'table': table})
                continue
            fields = catalog.get('fields', {}).get(table, [])
            names = [field['name'] for field in fields]
            sorted_rows = sorted(_project_rows(raw_rows, names),
                                 key=lambda row: row['row_id'])
            actual_rows = queried[table]['rows']
            if actual_rows != sorted_rows:
                problems.append({'reason': '仓库查询结果与快照不符', 'table': table})
            if sorted(queried[table]['columns']) != sorted(
                    field['name'] for field in fields):
                problems.append({'reason': '仓库字段目录与目录记录不符',
                                 'table': table})
            if [field['name'] for field in fields][:len(snapshot['schema'])] \
                    != list(snapshot['schema']):
                problems.append({'reason': '字段目录未覆盖schema', 'table': table})
            manifest_entry = manifest.get('closure', {}).get(table, {})
            if manifest_entry.get('snapshot') != reference['snapshot'] \
                    or manifest_entry.get('data') != reference['data']:
                problems.append({'reason': '清单引用来源不符', 'table': table})
            if manifest_entry.get('data_digest') != digest_bytes(
                    self.store.get(reference['data'])):
                problems.append({'reason': '数据摘要不符', 'table': table})
            if snapshot['schema'] != reference['schema']:
                problems.append({'reason': '字段目录与计划不符', 'table': table})
            recorded = catalog['tables'].get(table, {})
            if recorded.get('schema') != reference['schema'] \
                    or recorded.get('snapshot') != reference['snapshot'] \
                    or recorded.get('data') != reference['data'] \
                    or recorded.get('rows') != len(raw_rows):
                problems.append({'reason': '目录记录与计划不符', 'table': table})
            row_digest = digest_value(sorted_rows)
            order = ' ORDER BY "row_id"' if 'row_id' in queried[table]['columns'] else ''
            projection = (', '.join(_quote(column)
                                    for column in queried[table]['columns'])
                          if queried[table]['columns'] else 'NULL')
            checks.append({
                'table': table,
                'snapshot': reference['snapshot'],
                'data': reference['data'],
                'schema': snapshot['schema'],
                'fields': fields,
                'schema_digest': digest_value(snapshot['schema']),
                'data_digest': digest_bytes(self.store.get(reference['data'])),
                'row_digest': row_digest,
                'rows': len(raw_rows),
                'row_ids': sorted(row['row_id'] for row in raw_rows),
                'references': {
                    'baseline_archive': manifest.get('baseline', {}).get('archive_path'),
                    'baseline_archive_digest': manifest.get('baseline', {}).get(
                        'archive_digest'),
                    'backup_id': manifest.get('baseline', {}).get('backup_id'),
                    'base_commit': manifest.get('baseline', {}).get('base_commit'),
                    'commits': manifest.get('commits'),
                    'log_head': manifest.get('log_head'),
                    'target': self.plan['target'],
                    'plan_digest': self.plan['plan_digest'],
                },
                'query': {
                    'sql': f'SELECT {projection} FROM {_quote(table)}{order}',
                    'rows_returned': len(actual_rows),
                    'rows_digest': digest_value(actual_rows),
                },
            })
            result_rows[table] = actual_rows
        if problems:
            raise RestoreError('恢复仓校验失败', {
                'target': self.plan['target'], 'checks': checks,
                'blocked': {'stage': 'verify', 'reason': '校验不通过',
                            'problems': problems}})
        verification = {
            'format': FORMAT, 'plan_digest': self.plan['plan_digest'],
            'target': self.plan['target'],
            'manifest_digest': digest_value(manifest),
            'database': 'warehouse.sqlite3',
            'database_digest': _artifact_digest(self.directory, 'warehouse.sqlite3'),
            'tables': checks,
            'objects': manifest.get('objects', []),
        }
        digest = _atomic_write(self.directory / 'verification.json',
                               encode(verification))
        _write_marker(self.directory, self.plan['plan_digest'], 'verified', {
            'verification.json': digest,
            'warehouse.sqlite3': _artifact_digest(self.directory,
                                                  'warehouse.sqlite3'),
        })
        return {
            'target': self.plan['target'],
            'plan_digest': self.plan['plan_digest'],
            'requested_target': self.plan.get('requested_target'),
            'tables': checks,
            'rows': result_rows,
            'verification': verification,
            'stages': stage_status(self.directory, self.plan['plan_digest']),
        }

    # -- 总流程 -------------------------------------------------------------

    def status(self):
        return {stage: {'complete': self.stage_complete(stage)[0],
                        'reason': self.stage_complete(stage)[1]}
                for stage in STAGES}

    def _run_stage(self, stage, action):
        complete, _ = self.stage_complete(stage)
        if not complete:
            return action()
        if stage == 'objects':
            artifacts = self._objects_artifacts()
        elif stage == 'tables':
            artifacts = self._tables_artifacts()
        elif stage == 'manifest':
            artifacts = {name: _artifact_digest(self.directory, name)
                         for name in ('manifest.json', 'index.json')}
        else:
            artifacts = {'verification.json': _artifact_digest(
                self.directory, 'verification.json'),
                'warehouse.sqlite3': _artifact_digest(self.directory,
                                                      'warehouse.sqlite3')}
        return _write_marker(self.directory, self.plan['plan_digest'], stage,
                             {k: v for k, v in artifacts.items() if v})

    def run(self):
        self._assert_isolated()
        self.directory.mkdir(parents=True, exist_ok=True)
        _state_root(self.directory).mkdir(parents=True, exist_ok=True)
        if fcntl is None:
            return self._execute()
        # 独占锁在进程崩溃时由内核释放，不会留下需要人工清理的锁文件。
        handle = os.open(_state_root(self.directory) / '.lock',
                         os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RestoreError('恢复目录正被另一个进程使用', {'blocked': {
                    'stage': 'run', 'reason': '并发恢复被拒绝',
                    'directory': str(self.directory)}})
            return self._execute()
        finally:
            os.close(handle)

    def _execute(self):
        self._clear_temporary_files()
        self._run_stage('objects', self.stage_objects)
        self._run_stage('tables', self.materialize)
        self._run_stage('manifest', self.write_manifest)
        # 校验阶段每次都重新查询数据库，确保返回的是本次打开仓库的结果。
        return self.verify()


# ---------------------------------------------------------------------------
# 便捷入口与打开恢复仓


def _resolve_alias(kwargs, names, default=None):
    for name in names:
        if name in kwargs:
            return kwargs.pop(name)
    return default


def restore(source=None, target=None, archive=None, log=None, directory=None,
            allow_fallback=False, **kwargs):
    """规划并执行恢复，返回校验结果。

    全量归档里的基线对象不需要预先复制进来源对象存储；执行时会从只读归档读取。
    常见的参数别名也可用：baseline/baseline_archive/full、increments/entries、
    destination/output/out/dir。
    """
    source = _resolve_alias(kwargs, ('store', 'objects', 'object_store', 'data'),
                            source)
    target = _resolve_alias(kwargs, ('commit', 'commit_id', 'restore_to',
                                     'target_commit'), target)
    archive = _resolve_alias(kwargs, ('baseline', 'baseline_archive', 'full',
                                      'archive_path', 'base'), archive)
    log = _resolve_alias(kwargs, ('increments', 'entries', 'journal', 'logs',
                                  'increment_log'), log)
    directory = _resolve_alias(kwargs, ('destination', 'output', 'out', 'dir',
                                        'target_directory', 'target_path'), directory)
    allow_fallback = kwargs.pop('fallback', allow_fallback)
    workdir = kwargs.pop('workdir', None)
    if kwargs:
        raise TypeError(f'未知参数：{sorted(kwargs)}')
    if isinstance(source, dict) and 'baseline' in source and archive is None:
        # 直接给合成资料文档：在临时目录构造来源，恢复完成后清理。
        temporary = None
        if workdir is None:
            temporary = tempfile.TemporaryDirectory(prefix='warehouse-source-')
            workdir = temporary.name
        try:
            built = build_source(source, workdir)
            return restore(built['store'], target, built['archive'],
                           built['log'], directory,
                           allow_fallback=allow_fallback)
        finally:
            if temporary is not None:
                temporary.cleanup()
    if source is None or target is None or archive is None or directory is None:
        raise TypeError('restore 需要 source/target/archive/directory')
    _, archive_objects = read_baseline(archive)
    isolated = _as_source(source, archive_objects)
    plan = plan_restore(isolated, target, archive, log or [],
                        allow_fallback=allow_fallback)
    return RestoreRun(plan, isolated, directory).run()


restore_chain = restore


class RestoredWarehouse:
    """打开恢复目录里的仓库，按表查询原始行并读取校验材料。"""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.manifest = _read_json(self.directory / 'manifest.json')
        self.catalog = _read_json(self.directory / 'catalog.json')
        self.verification = _read_json(self.directory / 'verification.json')
        if not isinstance(self.manifest, dict) or not isinstance(self.catalog, dict):
            raise ValueError('恢复仓不完整')
        if self.manifest.get('plan_digest') != self.catalog.get('plan_digest'):
            raise ValueError('恢复仓清单与目录不属于同一计划')
        if not (self.directory / 'warehouse.sqlite3').is_file():
            raise ValueError('恢复仓数据库缺失')

    @property
    def target(self):
        return self.manifest.get('target')

    @property
    def plan_digest(self):
        return self.manifest.get('plan_digest')

    @property
    def tables(self):
        return sorted(self.catalog.get('tables', {}))

    def fields(self, table):
        return self.catalog['tables'][table]['fields']

    def schema(self, table):
        return self.catalog['tables'][table]['schema']

    def rows(self, table):
        queried = _query_database(self.directory / 'warehouse.sqlite3',
                                  self.catalog.get('fields'))
        if table not in queried:
            raise KeyError(table)
        return queried[table]['rows']

    query = rows

    def references(self, table):
        return self.catalog['tables'][table]


def open_restored(directory):
    """打开恢复仓，返回可查询对象。"""
    return RestoredWarehouse(directory)


def query_restored(directory, table):
    return RestoredWarehouse(directory).rows(table)


def verify_restored(directory):
    """只凭恢复目录复核：重新打开数据库，对照清单引用对象逐表比较原始行。"""
    warehouse = RestoredWarehouse(directory)
    store = ObjectStore(warehouse.directory / 'objects')
    queried = _query_database(warehouse.directory / 'warehouse.sqlite3',
                              warehouse.catalog.get('fields'))
    if set(queried) != set(warehouse.tables):
        raise RestoreError('恢复仓表集合不符', {'blocked': {
            'stage': 'verify', 'reason': '表集合不符'}})
    checks = []
    for table in warehouse.tables:
        entry = warehouse.manifest['closure'][table]
        rows = Snapshots(store).read(entry['snapshot'])['rows']
        names = [field['name'] for field in warehouse.fields(table)]
        sorted_rows = sorted(_project_rows(rows, names),
                             key=lambda row: row['row_id'])
        if queried[table]['rows'] != sorted_rows:
            raise RestoreError('恢复仓行内容不符', {'blocked': {
                'stage': 'verify', 'table': table, 'reason': '查询结果与快照不符'}})
        if digest_bytes(store.get(entry['data'])) != entry['data_digest']:
            raise RestoreError('恢复仓数据摘要不符', {'blocked': {
                'stage': 'verify', 'table': table, 'reason': '数据摘要不符'}})
        if warehouse.verification is not None \
                and warehouse.verification.get('plan_digest') != warehouse.plan_digest:
            raise RestoreError('恢复仓校验材料属于另一份计划', {'blocked': {
                'stage': 'verify', 'reason': '校验材料计划摘要不符'}})
        checks.append({'table': table, 'snapshot': entry['snapshot'],
                       'data_digest': entry['data_digest'],
                       'rows': len(sorted_rows),
                       'row_digest': digest_value(sorted_rows),
                       'fields': warehouse.fields(table),
                       'references': {'target': warehouse.target,
                                      'commits': warehouse.manifest.get('commits'),
                                      'log_head': warehouse.manifest.get('log_head'),
                                      'plan_digest': warehouse.plan_digest}})
    return {'target': warehouse.target,
            'plan_digest': warehouse.plan_digest,
            'tables': checks,
            'rows': {table: queried[table]['rows'] for table in warehouse.tables}}


# ---------------------------------------------------------------------------
# 从合成资料文档构造并恢复（便于演示/回归）


def _document_heads(store, tables):
    heads = {}
    for name, value in tables.items():
        if not isinstance(value, dict) or 'rows' not in value or 'schema' not in value:
            raise RestoreError('资料表非法', {'blocked': {
                'stage': 'source', 'reason': '表缺少 rows/schema', 'table': name}})
        digest = Snapshots(store).create(value['rows'], value['schema'])
        heads[name] = digest
        heads.setdefault(digest, digest)  # 允许用摘要直接引用
    return heads


def build_source(data, workdir, archive_name='baseline.tar'):
    """从 `{baseline, increments, tables}` 文档构造来源。

    `tables` 的键可以是版本名（如 `u1`），也可以是 64 位摘要；`baseline` 与增量
    `changes` 里的版本名会解析为快照摘要。缺少 `parent_digest` 时按规范摘要补全。
    返回 `{'store', 'archive', 'log'}`，对象都在 `workdir/objects` 中。
    """
    if not isinstance(data, dict):
        raise RestoreError('资料文档非法', {'blocked': {
            'stage': 'source', 'reason': '文档不是对象'}})
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    store = ObjectStore(workdir / 'objects')
    heads = _document_heads(store, data.get('tables', {}))
    baseline = data.get('baseline')
    if not isinstance(baseline, dict):
        raise RestoreError('资料文档缺少基线', {'blocked': {
            'stage': 'baseline', 'reason': '缺少 baseline'}})
    resolved_heads = {}
    for table, version in baseline.get('table_heads', {}).items():
        resolved = version if is_digest(version) else heads.get(version)
        if not is_digest(resolved):
            raise RestoreError('基线版本未知', {'blocked': {
                'stage': 'baseline', 'reason': '基线表版本不存在',
                'table': table, 'version': version}})
        resolved_heads[table] = resolved
    baseline_value = dict(baseline, table_heads=resolved_heads)
    digests = []
    for head in resolved_heads.values():
        digests.append(head)
        digests.append(json.loads(store.get(head))['data'])
    archive_path = workdir / archive_name
    create_archive(store, digests, baseline_value, archive_path)
    previous = digest_value({'backup_id': baseline_value['backup_id'],
                             'base_commit': baseline_value['base_commit'],
                             'table_heads': resolved_heads})
    log = []
    for raw in data.get('increments', []):
        changes = {}
        for table, version in raw.get('changes', {}).items():
            if version is None or (isinstance(version, dict)
                                   and (version.get('deleted') or version.get('delete'))):
                changes[table] = None
            elif is_digest(version):
                changes[table] = version
            elif isinstance(version, str) and version in heads:
                changes[table] = heads[version]
            else:
                raise RestoreError('增量版本未知', {'blocked': {
                    'stage': 'log', 'reason': '变更表版本不存在',
                    'table': table, 'version': version,
                    'sequence': raw.get('sequence')}})
        entry = {'sequence': raw.get('sequence'), 'commit_id': raw.get('commit_id'),
                 'changes': changes}
        entry['parent_digest'] = raw.get('parent_digest', previous)
        log.append(entry)
        previous = canonical_entry_digest(entry)
    archive = {'path': archive_path,
               'backup_id': baseline_value['backup_id'],
               'base_commit': baseline_value['base_commit'],
               'table_heads': resolved_heads}
    return {'store': store, 'archive': archive, 'log': log}


def read_inputs(archive_path, log_path, objects_root):
    """读取现成的三份输入：全量归档、增量日志、摘要寻址对象目录。

    日志可以是数组，或含 `increments` 的对象；只读取，不写这两个位置。
    """
    store = ObjectStore(objects_root)
    log_raw = json.loads(Path(log_path).read_bytes())
    if isinstance(log_raw, dict):
        if 'increments' in log_raw:
            log = log_raw['increments']
        elif 'log' in log_raw:
            log = log_raw['log']
        else:
            log = log_raw
    else:
        log = log_raw
    return {'store': store, 'archive': {'path': str(archive_path)}, 'log': log}


def restore_from_files(archive_path, log_path, objects_root, target, directory,
                       allow_fallback=False):
    """按归档、日志、对象目录三份输入恢复。"""
    inputs = read_inputs(archive_path, log_path, objects_root)
    return restore(inputs['store'], target, inputs['archive'], inputs['log'],
                   directory, allow_fallback=allow_fallback)


def plan_from_files(archive_path, log_path, objects_root, target,
                    allow_fallback=False):
    inputs = read_inputs(archive_path, log_path, objects_root)
    return plan_restore(inputs['store'], target, inputs['archive'],
                        inputs['log'], allow_fallback=allow_fallback)


def plan_document(data, target, workdir, allow_fallback=False):
    """从资料文档构造来源并规划；`workdir` 由调用方持有。"""
    source = build_source(data, workdir)
    return plan_restore(source['store'], target, source['archive'], source['log'],
                        allow_fallback=allow_fallback)


def restore_document(data, target, directory, workdir=None, allow_fallback=False):
    """从 `{baseline, increments, tables}` 文档恢复。

    未提供 `workdir` 时使用临时目录构造来源，恢复完成后清理；恢复结果全部在
    `directory` 中，可随后用 `open_restored` 打开查询。
    """
    temporary = None
    if workdir is None:
        temporary = tempfile.TemporaryDirectory(prefix='warehouse-source-')
        workdir = temporary.name
    try:
        source = build_source(data, workdir)
        return restore(source['store'], target, source['archive'], source['log'],
                       directory, allow_fallback=allow_fallback)
    finally:
        if temporary is not None:
            temporary.cleanup()


def diagnose_restore(source, target, archive, log, allow_fallback=False):
    """只读核对但不抛异常：返回 `{'ok', 'report', 'plan'}`。"""
    try:
        plan = plan_restore(source, target, archive, log,
                            allow_fallback=allow_fallback)
    except RestoreError as error:
        report = dict(error.report)
        report.setdefault('message', str(error))
        return {'ok': False, 'report': report, 'plan': None}
    return {'ok': True, 'report': plan['report'], 'plan': plan}


class Restorer:
    """把来源、归档和日志绑在一起，按目标规划/执行/查看进度。"""

    def __init__(self, source, archive, log, directory=None,
                 allow_fallback=False):
        self.source = source
        self.archive = archive
        self.log = log
        self.directory = directory
        self.allow_fallback = allow_fallback

    def plan(self, target, **kwargs):
        return plan_restore(self.source, target, self.archive, self.log,
                            allow_fallback=self.allow_fallback, **kwargs)

    def restore(self, target, directory=None):
        destination = directory if directory is not None else self.directory
        if destination is None:
            raise TypeError('restore 需要目标目录')
        return restore(self.source, target, self.archive, self.log, destination,
                       allow_fallback=self.allow_fallback)

    def status(self, plan, directory=None):
        destination = directory if directory is not None else self.directory
        return stage_status(destination, plan['plan_digest'])

    def completed(self, plan, directory=None):
        destination = directory if directory is not None else self.directory
        return completed_stages(destination, plan['plan_digest'])


# 语义别名：同一实现的不同叫法，便于既有调用方迁移。
recover = restore
restore_to = restore
restore_target = restore
restore_chain_to = restore
resume = restore
plan = plan_restore
plan_chain = plan_restore
open_repository = open_restored
open_warehouse = open_restored
verify_repository = verify_restored
verify_restore = verify_restored
previous_stages = completed_stages
restore_status = stage_status
resume_status_for = stage_status
plan_from_document = plan_document
check_restore = diagnose_restore
check_chain = diagnose_restore

__all__ = [
    'FORMAT', 'PLAN_FORMAT', 'STAGES', 'RestoreError', 'ChainError',
    'RecoveryError', 'Source', 'RestoreRun', 'RestoredWarehouse', 'Restorer',
    'is_digest', 'is_table_name', 'digest_bytes', 'digest_value',
    'read_baseline', 'baseline_candidates', 'read_chain', 'inspect_closure',
    'plan_restore', 'plan_body', 'diagnose_restore', 'restore',
    'restore_chain', 'recover', 'restore_to', 'restore_target',
    'restore_chain_to', 'resume', 'plan', 'plan_chain',
    'open_restored', 'open_repository', 'open_warehouse', 'query_restored',
    'verify_restored', 'verify_repository', 'verify_restore',
    'stage_status', 'resume_status', 'restore_status', 'completed_stages',
    'previous_stages', 'save_plan', 'load_plan', 'build_source',
    'restore_document', 'plan_document', 'plan_from_document',
    'read_inputs', 'restore_from_files', 'plan_from_files',
    'check_restore', 'check_chain', 'canonical_entry_digest',
]

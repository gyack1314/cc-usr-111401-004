# 按已提交位置恢复

`warehouse/restore.py` 在既有单包归档之上实现增量链恢复。它先只读核对，再在隔离
目录生成可打开的仓库；任何阶段都不会写入正在提供查询的来源仓库。

## 输入

* **全量归档**：由 `backup.create_archive` 生成，含 `backup_id`、`base_commit`、
  `table_heads` 和 `objects`。归档成员摘要由 `read_archive` 校验。
* **增量日志**：有序数组（或按 `sequence` 编号的映射），每项是一笔完整已提交事务：
  `sequence`、`commit_id`、`changes`、`parent_digest`。`changes` 把表名替换为不可变
  快照摘要；值为 `null`（或 `{"deleted": true}`）表示删除该表。
* **对象目录**：SHA256 寻址的不可变对象，可用 `ObjectStore` 读取。

日志链的起点父摘要，规范写法是
`sha256(canonical_json({"backup_id", "base_commit", "table_heads"}))`；后一项的
`parent_digest` 指向前一项的
`sha256(canonical_json({"sequence", "commit_id", "changes"}))`。为兼容既有日志，
读取端也接受归档清单摘要、表头映射摘要等常见链接写法；缺少 `parent_digest`
字段时按规范链摘要补全，字段存在但值不符则视为断裂。

## 两步接口

```python
from warehouse.objects import ObjectStore
from warehouse.restore import (plan_restore, RestoreRun, open_restored)

store = ObjectStore('objects')
plan = plan_restore(store, 'c2', 'full.tar', log)   # 只读；失败抛 RestoreError
run = RestoreRun(plan, store, 'restored')           # 可重复调用以续做
result = run.run()
warehouse = open_restored('restored')
warehouse.query('usage')                            # 真正打开仓库查询原始行
```

也可以用三份现成输入：`restore_from_files(archive, log_json, objects_root,
target, directory)`；或直接给含 `baseline/increments/tables` 的合成资料文档
（`restore_document(data, target, directory)`）。

`plan_restore` 的返回值可 JSON 序列化，并带 `plan_digest`。计划摘要覆盖基线身份、
目标、提交步骤、表头、闭包与日志末端，因此同一目标得到同一计划；不同目标必然
得到不同计划。可用 `save_plan` / `load_plan` 落盘与复核。

## 完整性规则

1. **编号与链接**：`sequence` 必须连续递增；父摘要必须匹配前一项。编号连续而链接
   断裂、编号缺失、提交身份重复、变更表名或摘要非法，都立即阻塞。
2. **完整提交边界**：一笔提交里的多张表一起生效。目标之前的提交必须整笔可物化
   （每张被替换表的快照与数据对象都在闭包内）；目标之后的提交（删除、新表、新
   schema）一律不参与重建。
3. **基线核对**：归档可读、成员摘要正确、元数据与声明一致、基线表头在归档对象
   清单中，且快照与数据对象可读。基线闭包损坏时没有可恢复末端。
4. **对象闭包**：恢复目标状态下每张表的快照清单、数据对象、行身份与行数都核对。

## 不可恢复时的裁决

`RestoreError.report` 是稳定结构，关键字段：

* `recoverable_end` / `last_recoverable_commit`：最后一个可完整恢复的提交。
* `blocked.stage`：`baseline` / `log` / `commit` / `target` / `closure` /
  `objects` / `tables` / `manifest` / `verify` / `run`。
* `blocked` 里的定位字段：`commit_id`、`sequence`、`position`、
  `after_sequence`、`object`、`table`、`field`、`missing`、`broken`。
* `blocked.reason`：如 `父摘要链接断裂`、`日志编号不连续`、`基线不匹配...`、
  `对象闭包不完整`。
* `reachable`：可靠前缀中的提交；`applied` / `ignored`：计划应用与忽略的提交。

默认不会“降级恢复”；`allow_fallback=True` 才退到 `recoverable_end`，并在计划里
记录 `requested_target` 与 `fallback`。

## 崩溃续做与阶段标记

阶段顺序：`objects` → `tables` → `manifest` → `verified`。每个阶段把产物写到
临时文件、`fsync`、原子改名，最后才写标记。标记路径
`.restore-state/<plan_digest>/<stage>.json`，内容同时记录计划摘要和各产物摘要。

* `stage_status(directory, plan_digest)` 返回每阶段 `{complete, reason}`。
* `completed_stages(...)` 返回可靠完成的阶段列表。
* 标记缺失但产物内容完整时，阶段被识别为“产物完整，补写标记”；标记存在但产物
  被改坏时，阶段回到未完成并重建。
* 换恢复目标只会在新计划摘要名下留标记；旧计划的标记不会被新计划当作完成状态。
* 同一目录的并发恢复用 `flock` 互斥；进程崩溃时锁由内核释放。

## 恢复产物与校验

恢复目录包含：

* `objects/`：计划闭包内的内容对象（复用前逐一复核摘要）。
* `tables/<表>.json`：按表快照的 `{schema, rows}`。
* `refs.json`：表名 → 目标快照摘要。
* `catalog.json`：每张表的字段目录（`name` / `declared` / `kind` / `sql_type`）、
  快照摘要、数据摘要、行身份与行数。
* `manifest.json`：基线身份、提交序列、`log_head`、表头、对象闭包与每表摘要。
* `index.json`：恢复清单摘要。
* `warehouse.sqlite3`：真正可打开的仓库，`row_id` 唯一排序。
* `verification.json`：每张表的字段目录、schema/数据/行摘要、查询 SQL、返回行数、
  行摘要与引用来源（备份 id、基线归档摘要、提交序列、日志末端、计划摘要）。

最终校验会重新打开数据库，逐表执行查询，把查询返回的原始行与不可变快照逐值比较，
并交叉核对表文件、目录记录、清单引用与引用来源。只“包存在”“对象复制完”或“行数
相同”都不能通过。

## 命令行

```sh
python -m warehouse plan --target c2 --save plan.json
python -m warehouse restore --target c2 --directory restored --workdir source
python -m warehouse status --plan plan.json --directory restored
python -m warehouse open --directory restored --table usage
python -m warehouse verify --directory restored
```

三件套输入用 `--archive full.tar --log increments.json --objects objects/`。

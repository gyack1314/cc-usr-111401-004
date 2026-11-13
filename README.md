# 仓库增量备份链资料工具

Python 3.11 项目，用于读取、保存及核对协作资料仓的本地资料。不可变内容通过 SHA256 寻址，目录引用单独保存。现有领域接口位于 warehouse/backup.py，资料契约见 docs/contracts.md，合成输入位于 data/source.json。warehouse/objects.py 负责内容校验，catalog.py 负责本地引用更新，snapshot.py 提供完整行快照基础读取。

已有读取流程使用临时目录，不改变源资料。除基础读取外，`warehouse/restore.py` 提供
按已提交位置恢复增量链：只读核对全量基线、日志编号与父摘要链接、目标状态的对象
闭包，再在隔离目录重建仓库；恢复点只停在完整提交边界，可崩溃续做，并输出每张表、
字段目录与引用来源的校验材料。用法见 `docs/restore.md`。

运行与开发检查：

```sh
python -m unittest discover -s tests -v
python -m compileall -q warehouse tests
python -m warehouse
```

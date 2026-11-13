# 资料与既有接口约定

全量归档有 backup_id、base_commit、table_heads 和 objects；增量项有 sequence、parent_digest、commit_id、changes，每项代表一笔完整已提交事务。changes 是表名到不可变目录摘要的替换，不执行任意SQL。每张表目录包含 schema 和 data 对象引用。目标用 commit_id 指定，不以时间戳猜测位置。backup.py 仅能创建、校验及读取单个归档对象；未实现链恢复。

所有资料均为合成，字段、身份及状态按本文解释。源码中的 inspect 入口只读取并核对现有基础接口；不会建立高级查询或仓库管理流程。

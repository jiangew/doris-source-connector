# Doris Source Connector 设计文档（Kafka Connect）

## 1. 项目背景

本项目目标是实现一个 **Kafka Connect Source Connector**，
用于将 **Apache Doris 中的数据变更（增量）持续写入 Kafka Topic**，
供下游系统（Flink / OLAP / Cache / 搜索 / 业务系统）订阅消费。

> ⚠️ 说明
> Apache Doris 当前 **不提供原生行级 CDC（binlog/WAL）能力**，
> 本方案采用 **工程可控的“增量扫描 + 幂等分发”模型**，
> 提供 **Effectively-once（准 Exactly-once）** 语义。

---

## 2. 总体架构

```
Apache Doris (UNIQUE KEY 表)
        |
   增量拉取（Pull）
        |
Kafka Connect SourceTask
        |
   Kafka Producer
        |
   Kafka Topic（下游订阅）
```

核心思想：

* Doris = 状态存储（State Store）
* Kafka = 变更分发日志（Change Feed）
* Source Connector = 增量同步引擎

---

## 3. Doris 表设计前提（强制）

### 3.1 表模型

* **UNIQUE KEY 表**
* Merge-on-Write 开启
* 必须包含单调递增字段

```sql
CREATE TABLE example_table (
  id           BIGINT,
  data         STRING,
  update_time  DATETIME,
  seq          BIGINT
)
UNIQUE KEY(id)
PROPERTIES (
  "enable_unique_key_merge_on_write" = "true"
);
```

### 3.2 字段语义

| 字段          | 作用                 |
| ----------- | ------------------ |
| id          | 业务主键               |
| seq         | 单调递增版本号（offset 基础） |
| update_time | Merge 可见性防抖        |

---

## 4. Kafka Connect 工程结构

```
org.apache.doris.connect.source
├── DorisSourceConnector
├── DorisSourceTask
├── DorisSourceConfig
├── DorisSourceOffset
├── client/
│   └── DorisClient
├── fetcher/
│   └── DorisIncrementalFetcher
├── converter/
│   └── DorisRecordConverter
└── model/
    └── DorisRow
```

---

## 5. Source Connector 核心语义

### 5.1 增量模型

```sql
SELECT *
FROM table
WHERE seq > last_seq
ORDER BY seq
LIMIT batch_size
```

* seq 单调递增
* 查询可重放
* 允许重复，不允许丢失

---

## 6. Effectively-once 语义设计（关键）

### 6.1 基本原则

* Source Connector **不支持严格 Exactly-once**
* 目标：**不丢数据 + 可重复 + 可恢复**
* Offset 只由 Kafka Connect 提交

### 6.2 Offset 提交时机（正确）

```
poll()
  → return SourceRecord
    → Kafka 写入成功
      → Kafka Connect 提交 offset
```

❌ 严禁在 poll 内主动提交 offset

---

## 7. Offset 设计

### 7.1 SourcePartition（Task 隔离）

```json
{
  "db": "db_name",
  "table": "table_name",
  "task_id": "0",
  "task_count": "3"
}
```

### 7.2 Offset 内容

```json
{
  "last_seq": 123456789
}
```

* 单调递增
* 每个 Task 独立维护

---

## 8. 多 Task 并行模型（Hash Partition Offset）

### 8.1 并行原则

```
hash(primary_key) % task_count == task_id
```

每个 Task 只负责自己分片的数据。

### 8.2 并行 SQL 示例

```sql
SELECT *
FROM table
WHERE seq > ?
  AND MOD(id, ?) = ?
ORDER BY seq
LIMIT ?
```

参数：

1. last_seq
2. task_count
3. task_id
4. batch_size

---

## 9. 失败恢复与重放语义

### 9.1 允许的行为

| 场景                   | 结果    |
| -------------------- | ----- |
| Task 崩溃              | 数据可重放 |
| Kafka 写入后 offset 未提交 | 产生重复  |
| Rebalance            | 局部重扫  |
| tasks.max 变更         | 重复增加  |

✅ **绝不丢数据**

---

## 10. Doris Merge 延迟处理（必做）

### 10.1 问题

* UNIQUE KEY Merge 是异步的
* 可能读到旧版本

### 10.2 解决方案（推荐）

```sql
WHERE seq > last_seq
  AND update_time < now() - interval 5 second
```

* 引入安全延迟窗口
* 保证版本可见性

---

## 11. Kafka Topic 设计建议

* **Key**：Doris UNIQUE KEY
* **Value**：完整行数据（JSON / Avro）

```json
{
  "op": "UPSERT",
  "seq": 123,
  "data": { ... }
}
```

---

## 12. 下游消费要求（必须声明）

| 系统     | 要求           |
| ------ | ------------ |
| Flink  | KeyBy + 去重   |
| OLAP   | 覆盖写 / Merge  |
| Cache  | 幂等更新         |
| Search | doc_id = key |

---

## 13. 语义声明（README 可直接引用）

> This connector provides **effectively-once delivery semantics**.
> It guarantees no data loss and allows reprocessing on failures.
> Downstream systems must support idempotent consumption.

---

## 14. 不支持范围（必须说明）

* ❌ 物理 DELETE
* ❌ 事务级顺序
* ❌ 严格 CDC（binlog 级）

---

## 15. 适用场景

* Doris → Kafka 实时分发
* Doris → Flink / OLAP / Cache
* 构建 Doris-based Change Feed

---

## 16. 总结

这是一个：

* 可扩展
* 可恢复
* 可解释
* 符合 Kafka Connect 设计哲学

的 **生产级 Doris Source Connector 架构方案**。

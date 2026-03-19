# Doris Source Connector (Kafka Connect)

## 1. 项目背景

本项目实现一个 **Kafka Connect Source Connector**，将 **Apache Doris** 的增量变更持续写入 Kafka Topic，供下游系统（Flink / OLAP / Cache / Search / 业务系统）订阅消费。

> ⚠️ 说明
> Apache Doris 当前 **不提供原生行级 CDC（binlog/WAL）能力**，
> 本方案采用 **工程可控的“增量扫描 + 幂等分发”模型**，
> 提供 **Effectively-once（准 Exactly-once）** 语义。

## 2. 总体架构

```text
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

- Doris = 状态存储（State Store）
- Kafka = 变更分发日志（Change Feed）
- Source Connector = 增量同步引擎

## 3. 构建与运行

### 编译与打包

确保已安装 Maven 3.6+ 和 JDK 8+。

```bash
# 运行单元测试
mvn test

# 打包（生成包含所有依赖的 fat JAR）
mvn clean package
```

构建成功后，JAR 文件位于 `target/doris-source-connector-0.1.0-SNAPSHOT.jar`。

### 安装到 Kafka Connect

将生成的 JAR 文件拷贝到 Kafka Connect 的 `plugin.path` 目录下，并重启 Kafka Connect。

## 4. 配置参数

| 参数名 | 默认值 | 重要程度 | 说明 |
| --- | --- | --- | --- |
| `doris.host` | - | HIGH | Doris FE 地址 |
| `doris.port` | 9030 | HIGH | Doris MySQL 协议查询端口 |
| `doris.user` | - | HIGH | 数据库用户名 |
| `doris.password` | - | HIGH | 数据库密码 |
| `doris.database` | - | HIGH | 数据库名 |
| `doris.table` | - | HIGH | 表名 |
| `seq.column` | `seq` | MEDIUM | 增量同步所依据的序列号列名（需单调递增） |
| `update.time.column` | `update_time` | MEDIUM | 安全延迟窗口字段（用于过滤未稳定行） |
| `safety.delay.ms` | `0` | LOW | 安全延迟窗口毫秒数（0 表示不启用） |
| `partition.column` | `id` | MEDIUM | 任务分片列（hash 分片） |
| `key.columns` | 空 | MEDIUM | Kafka Key 列（逗号分隔） |
| `topic.template` | `${database}.${table}` | MEDIUM | Topic 模板（支持占位符 `${database}`、`${table}`） |
| `batch.size` | 1000 | MEDIUM | 每次查询的最大行数 |
| `poll.interval.ms` | 5000 | LOW | 空结果时的休眠间隔 |

## 5. Doris 表设计前提（强制）

### 5.1 表模型

- **UNIQUE KEY 表**
- Merge-on-Write 开启
- 必须包含单调递增字段

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

### 5.2 字段语义

| 字段 | 作用 |
| --- | --- |
| `id` | 业务主键 |
| `seq` | 单调递增版本号（offset 基础） |
| `update_time` | Merge 可见性防抖 |

## 6. 核心语义设计

### 6.1 增量模型

```sql
SELECT *
FROM table
WHERE seq > last_seq
ORDER BY seq
LIMIT batch_size
```

- `seq` 单调递增
- 查询可重放
- 允许重复，不允许丢失

### 6.2 Effectively-once 语义

- 目标：**不丢数据 + 可重复 + 可恢复**
- Offset 只由 Kafka Connect 提交

```text
poll()
  → return SourceRecord
    → Kafka 写入成功
      → Kafka Connect 提交 offset
```

> ❌ 严禁在 poll 内主动提交 offset

## 7. 并行与分片模型

### 7.1 并行原则

```text
hash(primary_key) % task_count == task_id
```

每个 Task 只负责自己分片的数据。并行 SQL 示例：

```sql
SELECT *
FROM table
WHERE seq > ?
  AND MOD(id, ?) = ?
ORDER BY seq
LIMIT ?
```

## 8. 运维建议

### 8.1 Doris Merge 延迟处理

UNIQUE KEY Merge 是异步的，可能读到旧版本。推荐引入安全延迟窗口：

```sql
WHERE seq > last_seq
  AND update_time < now() - interval 5 second
```

### 8.2 Kafka Topic 设计

- **Key**：Doris UNIQUE KEY
- **Value**：完整行数据（JSON / Avro）

```json
{
  "op": "UPSERT",
  "seq": 123,
  "data": { "id": 1, "name": "foo", ... }
}
```

## 9. 下游消费要求

| 系统 | 要求 |
| --- | --- |
| Flink | KeyBy + 去重 |
| OLAP | 覆盖写 / Merge |
| Cache | 幂等更新 |
| Search | `doc_id` = `key` |

## 10. 不支持范围

- ❌ 物理 DELETE
- ❌ 事务级顺序
- ❌ 严格 CDC（binlog 级）

## 11. 适用场景

- Doris → Kafka 实时分发
- Doris → Flink / OLAP / Cache
- 构建 Doris-based Change Feed

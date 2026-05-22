# Hazelcast + DuckDB 高性能写入优化示例

本模块演示如何在 Hazelcast Jet 流处理作业中**极致优化** DuckDB 的写入性能。通过批量写入、连接复用、事务优化等技术手段，实现高吞吐的实时数据入库与宽表查询。

---

## 目录
- [功能概述](#功能概述)
- [性能优化亮点](#性能优化亮点)
- [代码层级结构](#代码层级结构)
- [核心调用逻辑](#核心调用逻辑)
- [电商测试数据说明](#电商测试数据说明)
- [快速开始](#快速开始)
- [配置参数](#配置参数)
- [压测方案](#压测方案)
- [输出说明](#输出说明)

---

## 功能概述

| 功能 | 说明 |
|------|------|
| 实时流处理 | 基于 Hazelcast Jet Pipeline 处理电商订单批次流 |
| 嵌入式存储 | 使用 DuckDB 作为嵌入式关系型存储，无需独立进程 |
| 宽表查询 | 支持 buyer/order/item 三表 JOIN 查询 |
| 高性能写入 | 批量 upsert、连接池复用、事务优化 |
| 实时统计 | 滚动统计与汇总报告，包含 QPS、延迟等指标 |
| 容错机制 | 自动重试处理 DuckDB catalog 冲突 |

---

## 性能优化亮点

### 1. 批量写入优化
- 使用 `executeBatch()` 批量插入订单项
- 预编译语句复用，减少 SQL 解析开销
- 关闭 WAL 自动检查点（`wal_autocheckpoint=0`）

### 2. 连接与事务优化
- 共享连接池，避免频繁创建连接
- 手动事务控制，减少提交开销
- 乐观锁重试机制处理并发冲突

### 3. DuckDB 配置调优
```
jdbc:duckdb:memory:?memory_limit=4GB&threads=8&wal_autocheckpoint=0&enable_object_cache=true
```

---

## 代码层级结构

```
src/main/java/com/hazelcast/samples/duckdb/
├── HazelcastDuckDbApplication.java    # 主入口，启动 Hazelcast 并运行作业
├── HazelcastDuckDbJob.java           # Jet Pipeline 构建与提交
├── EcommerceDuckDbOperator.java      # DuckDB 核心操作类（写入/查询）
├── EcommerceMockGenerator.java       # 电商测试数据生成器
├── PerfConfig.java                   # 性能配置常量
├── StatsCollector.java               # 性能统计收集器
├── ArrowSchemaFactory.java           # Arrow 格式支持（可选）
└── model/                            # 数据模型
    ├── EcommerceBuyer.java           # 买家实体
    ├── EcommerceOrder.java           # 订单主表实体
    ├── EcommerceOrderItem.java       # 订单项实体
    └── EcommerceOrderBatch.java      # 批次数据容器
```

### 类职责说明

| 类名 | 职责 | 核心方法 |
|------|------|----------|
| `HazelcastDuckDbApplication` | 应用入口 | `main()` - 初始化并运行 |
| `HazelcastDuckDbJob` | 构建 Pipeline | `run()` - 创建流源、处理、输出 |
| `EcommerceDuckDbOperator` | DuckDB 操作 | `processBatch()` - 事务式处理 |
| `EcommerceMockGenerator` | 数据生成 | `generateBatches()` - 生成模拟数据 |
| `PerfConfig` | 配置管理 | 常量定义 |
| `StatsCollector` | 统计收集 | `recordBatch()`, `printSummary()` |

---

## 核心调用逻辑

### 运行时流程

```
1) HazelcastDuckDbApplication.main() starts a Hazelcast instance.
2) EcommerceMockGenerator.generateBatches() creates EcommerceOrderBatch data.
3) HazelcastDuckDbJob.run() builds the pipeline and writes results to a list sink.
4) EcommerceDuckDbOperator.processBatch() writes data and runs the join query.
5) StatsCollector.printSummary() outputs throughput and latency metrics.
```

### 关键代码片段

**Pipeline 构建（HazelcastDuckDbJob.java）**
```java
StreamSource<EcommerceOrderBatch> source = buildStreamingSource(batches, repeatCount);
pipeline.readFrom(source)
    .withoutTimestamps()
    .mapUsingService(
        ServiceFactories.nonSharedService(ignored -> new EcommerceDuckDbOperator()),
        (operator, batch) -> operator.processBatch(batch)
    )
    .flatMap(Traversers::traverseIterable)
    .writeTo(Sinks.list(PerfConfig.RESULT_LIST_NAME));
```

**事务处理（EcommerceDuckDbOperator.java）**
```java
private List<Map<String, Object>> performTransaction(EcommerceOrderBatch batch) {
    // 1. 删除订单（级联删除 order_item 和 order_main）
    // 2. Upsert buyer_info (INSERT OR REPLACE)
    // 3. Upsert order_main
    // 4. Batch upsert order_item (executeBatch)
    // 5. 执行宽表查询
    // 6. commit
}
```

---

## 电商测试数据说明

### 数据模型

| 表名 | 说明 | 字段数 | 主键 |
|------|------|--------|------|
| `buyer_info` | 买家信息表 | 7 | buyer_id |
| `order_main` | 订单主表 | 15 | order_id |
| `order_item` | 订单项表 | 13 | item_id |

### 数据生成规则

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| 批次数量 | 1000 | `PerfConfig.DEFAULT_BATCH_COUNT` |
| 每批订单项数 | 2-5 | `ITEMS_PER_ORDER_MIN` ~ `ITEMS_PER_ORDER_MAX` |
| Random Seed | 20260515L | 固定种子保证可重复 |

### 字段取值范围

**买家信息 (buyer_info)**
- `buyer_level`: 普通会员 / 白银会员 / 黄金会员 / 钻石会员
- `register_area`: 广东省深圳市 / 浙江省杭州市 / 上海市 / 北京市 / 四川省成都市

**订单主表 (order_main)**
- `order_status`: 待付款 / 已付款 / 已发货 / 已完成 / 已取消 / 已退款
- `pay_way`: 支付宝 / 微信支付 / 银行卡 / 抖音分期 / 花呗
- `order_channel`: APP商城 / 微信小程序 / H5网页 / 直播带货 / 线下门店

**订单项 (order_item)**
- `category1`: 数码家电 / 服饰鞋包 / 食品生鲜 / 家居日用 / 美妆护肤
- `brand_name`: 华为 / 小米 / 苹果 / 耐克 / 李宁 / 三只松鼠 / 欧莱雅

---

## 快速开始

### 环境要求
- JDK 17+
- Maven 3.6+

### 构建

```bash
# 构建本模块及其依赖
mvn -pl hazelcast-duckdb -am clean package

# 构建并跳过测试
mvn -pl hazelcast-duckdb -am clean package -DskipTests
```

### 运行

```bash
# 方式1: Maven Exec 直接运行（推荐）
mvn -pl hazelcast-duckdb -am exec:java \
  -Dexec.mainClass=com.hazelcast.samples.duckdb.HazelcastDuckDbApplication \
  -Dexec.args="1"
  --add-opens=java.base/java.nio=ALL-UNNAMED 
  -Dduckdb.batch.count=1000 
  -Dduckdb.batch-writing.size=10000 
  -Dduckdb.jet.parallelism=4 
  -Dduckdb.source.realtime=true 
  -Dduckdb.generation.qps=100000 
  -Dduckdb.batch-writing.timeout-ms=600000 
  -Dduckdb.stats.rolling.enabled=true 
  -Dduckdb.stats.source.enabled=false 
  -Dduckdb.stats.table-detailed=false
  -Dduckdb.write.mode=arrow 
  -Dduckdb.mongo.external.enabled=true

# 方式2: 打包后运行
mvn -pl hazelcast-duckdb -am package
mvn -pl hazelcast-duckdb -am dependency:copy-dependencies
java -cp "target/hazelcast-duckdb-1.0-SNAPSHOT.jar:target/dependency/*" \
  com.hazelcast.samples.duckdb.HazelcastDuckDbApplication 5
```

---

## 配置参数

### 命令行参数
| 参数 | 位置 | 说明 |
|------|------|------|
| `repeatCount` | 第1个参数 | 流重复回放次数，0 表示无限 |

### 系统属性

| 属性名 | 默认值 | 说明 |
|--------|--------|------|
| `duckdb.jdbc.url` | `jdbc:duckdb:memory:?memory_limit=2GB&threads=4&wal_autocheckpoint=0&enable_object_cache=true` | DuckDB JDBC URL |
| `duckdb.stream.repeat-count` | 1 | 流重复次数（命令行优先） |
| `duckdb.stats.rolling.enabled` | true | 是否启用实时滚动统计 |

### 配置示例

```bash
# 自定义 DuckDB 内存限制和线程数
mvn -pl hazelcast-duckdb -am exec:java \
  -Dexec.mainClass=com.hazelcast.samples.duckdb.HazelcastDuckDbApplication \
  -Dduckdb.jdbc.url="jdbc:duckdb:memory:?memory_limit=4GB&threads=8&wal_autocheckpoint=0" \
  -Dexec.args="10"
```

---

## 压测方案

### 压测配置建议

| 场景 | 配置 | 预期目标 |
|------|------|----------|
| 高吞吐 | `repeatCount=10`, 1000批次 | 1000+ 批次/秒 |
| 长时间运行 | `repeatCount=0` (无限) | 稳定运行无内存泄漏 |
| 大数据量 | 调整 `DEFAULT_BATCH_COUNT=10000` | 验证批量写入性能 |

### 压测命令

```bash
# 标准压测：1000批次，回放5次
mvn -pl hazelcast-duckdb -am exec:java \
  -Dexec.mainClass=com.hazelcast.samples.duckdb.HazelcastDuckDbApplication \
  -Dduckdb.jdbc.url="jdbc:duckdb:memory:?memory_limit=4GB&threads=8" \
  -Dexec.args="5"

# 长时间稳定性测试（无限循环）
mvn -pl hazelcast-duckdb -am exec:java \
  -Dexec.mainClass=com.hazelcast.samples.duckdb.HazelcastDuckDbApplication \
  -Dduckdb.stats.rolling.enabled=true \
  -Dexec.args="0"
```

---

## 输出说明

### 实时滚动统计（每秒输出）

```
[实时统计] 最近1秒行数=1234, 最近5秒平均 QPS=1180.5
  buyer_info: 最近1秒=200 行, 最近5秒平均 QPS=198.3
  order_main: 最近1秒=200 行, 最近5秒平均 QPS=198.3
  order_item: 最近1秒=834 行, 最近5秒平均 QPS=783.9
  Join 总返回行数=12340
```

### 汇总统计（作业结束）

```
=== 吞吐与延时统计汇总 ===
总批次数: 1000
总输出行数: 3500
总耗时: 0.852 秒
QPS: 4108.0 (行/秒)
批次吞吐: 1173.71 批/秒
平均每批延迟: 0.852 毫秒
===============================

按表汇总:
  buyer_info: 行数=1000, QPS=1173.7, 平均每行延迟=0.001 毫秒
  order_main: 行数=1000, QPS=1173.7, 平均每行延迟=0.001 毫秒
  order_item: 行数=3500, QPS=4108.0, 平均每行延迟=0.000 毫秒
Join（宽表查询）: 行数=3500, QPS=4108.0
```

---

## 常见问题

### 1. DuckDB 内存不足
- 解决方案：增加 `memory_limit` 参数
  ```bash
  -Dduckdb.jdbc.url="jdbc:duckdb:memory:?memory_limit=8GB"
  ```

### 2. 并发写入冲突
- DuckDB 内部已实现重试机制（最多5次）
- 可通过调整线程数减少冲突：`threads=4`

### 3. 统计信息过多
- 关闭实时滚动统计：
  ```bash
  -Dduckdb.stats.rolling.enabled=false
  ```

---

## 性能对比

| 优化项 | 优化前 | 优化后 | 提升幅度 |
|--------|--------|--------|----------|
| 批量写入 | 逐行 INSERT | `executeBatch()` | ~3x |
| 连接管理 | 每次创建 | 连接复用 | ~2x |
| WAL | 自动检查点 | 关闭 | ~1.5x |
| 总提升 | - | - | **~5-10x** |

## MongoDB 外部表配置说明

| 属性名 | 默认值 | 说明 |
|--------|--------|------|
| `duckdb.mongo.external.enabled` | false | 启用 MongoDB 外部表模式（写入走 MongoDB API，查询走 DuckDB 外表） |
| `duckdb.mongo.uri` | `mongodb://localhost:27017` | MongoDB 连接串 |
| `duckdb.mongo.database` | `tapdata-develop` | MongoDB 数据库名 |
| `duckdb.mongo.schema` | `mongo_db` | DuckDB 外表 schema 别名 |
| `duckdb.mongo.collection.buyer` | `buyer_info` | 买家集合名 |
| `duckdb.mongo.collection.order` | `order_main` | 订单集合名 |
| `duckdb.mongo.collection.item` | `order_item` | 订单项集合名 |

## Arrow + MongoDB 外部表 JOIN 性能优化

### 1. MongoDB 索引（已在代码中自动创建）

当开启 `duckdb.mongo.external.enabled=true` 时，`ArrowModeDuckDbOperator` 会在启动时创建以下索引以加速 JOIN 和 upsert：

- `buyer_info`: `buyer_id`（唯一索引）
- `order_main`: `order_id`（唯一索引）、`buyer_id`（普通索引）
- `order_item`: `item_id`（唯一索引）、`order_id`（普通索引）

> 如需自定义索引策略，可在 `ArrowModeDuckDbOperator.ensureMongoIndexes()` 中调整。

### 2. 其他优化手段（按影响排序）

- 控制 JOIN 驱动表：优先让 `order_main` 作为驱动表，并缩小 `IN (...)` 的批量大小，避免外部表全表扫描。
- 降低列宽：JOIN 查询只选择必要字段，减少 Arrow/网络传输的列宽和解码成本。
- 分批查询：将 `order_id IN (...)` 拆分为多个小批次，降低单次 JOIN 的峰值内存与 Mongo 扫描时延。
- 参数化查询：将 `IN` 列表改为 `VALUES` 临时表或参数化绑定，减少 SQL 解析与字符串拼接开销。
- 调整 DuckDB 线程与内存：增大 `threads` 与 `memory_limit`，同时避免过高并发导致 Mongo 端压力。
- 减少写后立即读：必要时对 JOIN 查询引入微小延迟或队列，减少写入后查询导致的热点。
- 优化 MongoDB 连接池：提升 `maxPoolSize`，并尽量保持连接复用，避免频繁建立连接。
- 预估行数并限流：在高并发下控制 `order_id` 输入数量，避免一次性 JOIN 太大。

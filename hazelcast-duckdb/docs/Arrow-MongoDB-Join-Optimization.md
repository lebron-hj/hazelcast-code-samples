# Arrow + MongoDB 外部表 Join 查询性能优化指南

除了增加 MongoDB 索引外，还有以下多种优化手段：

---

## 📊 优化策略总览

| 策略 | 实现难度 | 性能提升 | 适用场景 |
|------|---------|---------|---------|
| **数据预加载与缓存** | ⭐⭐ | ⭐⭐⭐⭐⭐ | 频繁查询的数据 |
| **Arrow 格式优化** | ⭐⭐⭐ | ⭐⭐⭐ | 大规模数据传输 |
| **DuckDB 配置优化** | ⭐ | ⭐⭐⭐ | 所有场景 |
| **谓词下推与投影裁剪** | ⭐⭐ | ⭐⭐⭐⭐ | 所有查询场景 |
| **物化视图** | ⭐⭐ | ⭐⭐⭐⭐⭐ | 相对静态数据 |
| **并行查询** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | 超大规模数据 |
| **Parquet 格式存储** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | 数据相对静态 |

---

## 1. 数据预加载与缓存优化

### 1.1 本地表缓存

**适用场景**：频繁查询、数据变化频率不高

```sql
-- 将 MongoDB 数据导入本地 DuckDB 表
CREATE TABLE local_buyers AS 
SELECT buyer_id, buyer_name, email, status 
FROM mongo_db.tapdata.buyer;

CREATE TABLE local_orders AS 
SELECT order_id, buyer_id, order_date, total_amount 
FROM mongo_db.tapdata.order;

-- 在本地表建立索引（DuckDB 索引性能更好）
CREATE INDEX idx_local_buyers_id ON local_buyers(buyer_id);
CREATE INDEX idx_local_orders_buyer_id ON local_orders(buyer_id);

-- 使用本地缓存表进行 Join
SELECT b.buyer_name, COUNT(o.order_id) as order_count
FROM local_buyers b
JOIN local_orders o ON b.buyer_id = o.buyer_id
WHERE b.status = 'ACTIVE'
GROUP BY b.buyer_name;
```

### 1.2 增量更新缓存

**适用场景**：数据有明确时间戳字段，允许一定延迟

```sql
-- 只加载新数据
INSERT INTO local_buyers
SELECT * FROM mongo_db.tapdata.buyer
WHERE update_time > last_sync_time;

-- 定期全量更新（防止数据不一致）
TRUNCATE TABLE local_buyers;
INSERT INTO local_buyers SELECT * FROM mongo_db.tapdata.buyer;
```

---

## 2. DuckDB 配置参数优化

### 2.1 连接时配置

```java
Properties props = new Properties();
props.setProperty("duckdb.memory_limit", "4GB");
props.setProperty("duckdb.threads", "8");
props.setProperty("duckdb.enable_optimizer", "true");

Connection conn = DriverManager.getConnection("jdbc:duckdb:memory:", props);
```

### 2.2 SQL 动态配置

```sql
-- 设置线程数
SET threads = 8;

-- 设置内存限制
SET max_memory = '4GB';

-- 启用查询优化器
SET optimizer = true;

-- 选择 Join 算法
SET enable_hashjoin = true;
SET enable_mergejoin = true;
SET enable_nestloop = true;
```

---

## 3. 查询重写与优化

### 3.1 谓词下推优化

```sql
-- ❌ 不优化：先 Join 再过滤
SELECT * 
FROM mongo_db.table1 t1
JOIN mongo_db.table2 t2 ON t1.id = t2.t1_id
WHERE t1.status = 'ACTIVE' AND t2.amount > 100;

-- ✅ 优化后：先过滤再 Join
SELECT * 
FROM (SELECT * FROM mongo_db.table1 WHERE status = 'ACTIVE') t1
JOIN (SELECT * FROM mongo_db.table2 WHERE amount > 100) t2
ON t1.id = t2.t1_id;
```

### 3.2 投影裁剪（只取需要的列）

```sql
-- ❌ 不优化：读取所有字段
SELECT * FROM mongo_db.large_table;

-- ✅ 优化后：只读取 Join 和结果需要的字段
SELECT id, buyer_id, amount, create_time 
FROM mongo_db.large_table;
```

---

## 4. 物化视图策略

**适用场景**：查询结果相对稳定，数据变化频率较低

```sql
-- 创建物化视图
CREATE OR REPLACE MATERIALIZED VIEW mv_buyer_order_summary AS 
SELECT 
    b.buyer_id,
    b.buyer_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value
FROM local_buyers b
JOIN local_orders o ON b.buyer_id = o.buyer_id
GROUP BY b.buyer_id, b.buyer_name;

-- 使用物化视图（毫秒级响应）
SELECT * FROM mv_buyer_order_summary 
WHERE total_spent > 1000
ORDER BY total_spent DESC;

-- 刷新物化视图（源数据更新时执行）
REFRESH MATERIALIZED VIEW mv_buyer_order_summary;
```

---

## 5. Arrow 格式优化

### 5.1 批量大小调优

```java
// 调整 Arrow 批量大小以提高缓存命中率
public static final int OPTIMAL_BATCH_SIZE = 8192;  // 2的幂次方
public static final int BATCH_SIZE_LARGE = 16384;   // 更大的批量
```

### 5.2 压缩配置

```java
import org.apache.arrow.compression.CommonsCompressionFactory;

// 设置压缩选项
ArrowVectorCompressionOptions compressionOptions = 
    ArrowVectorCompressionOptions.builder()
        .setCompression(ArrowCompressionCodec.LZ4_FRAME)
        .setCompressionLevel(3)
        .build();
```

---

## 6. 使用 Parquet 格式（终极优化）

**适用场景**：数据相对静态，或可接受一定延迟

### 6.1 导出为 Parquet 格式

```java
// 步骤1: 使用 MongoDB Java 驱动导出数据
// 步骤2: 使用 DuckDB 或 Spark 转换为 Parquet
```

```sql
-- 使用 DuckDB 读取 Parquet（比外部表快 10-100x）
INSTALL parquet;
LOAD parquet;

SELECT * 
FROM read_parquet('orders.parquet') orders
JOIN read_parquet('buyers.parquet') buyers
ON orders.buyer_id = buyers.id;
```

### 6.2 Parquet 分区优化

```sql
-- 使用分区字段提高查询速度
SELECT * 
FROM read_parquet('orders/year=2024/month=05/*.parquet');
```

---

## 7. 查询计划分析与调优

### 7.1 查看查询计划

```sql
-- 查看查询执行计划（不实际执行）
EXPLAIN 
SELECT b.buyer_name, COUNT(o.order_id) as order_count
FROM local_buyers b
JOIN local_orders o ON b.buyer_id = o.buyer_id
GROUP BY b.buyer_name;

-- 分析实际执行时间
EXPLAIN ANALYZE 
SELECT b.buyer_name, COUNT(o.order_id) as order_count
FROM local_buyers b
JOIN local_orders o ON b.buyer_id = o.buyer_id
GROUP BY b.buyer_name;
```

---

## 8. 性能对比示例

| 方案 | 查询时间 | 适用场景 |
|------|---------|---------|
| 直接 MongoDB 外部表 Join | ~5000ms | 小数据量，实时性要求高 |
| 本地缓存 + 索引 Join | ~200ms | 数据可缓存，更新频率低 |
| 物化视图查询 | ~50ms | 数据相对静态，预聚合场景 |
| Parquet 本地文件 Join | ~10ms | 数据批量更新，查询频繁 |

---

## 9. 完整优化流程建议

```
1. 分析查询模式
   ↓
2. 应用基础优化
   ├─ 谓词下推
   ├─ 投影裁剪
   └─ DuckDB 配置调优
   ↓
3. 检查是否需要缓存
   ├─ 是 → 本地缓存 + 索引
   └─ 否 → 继续
   ↓
4. 评估物化视图
   ├─ 是 → 创建物化视图
   └─ 否 → 继续
   ↓
5. 终极优化（数据量大时）
   └─ Parquet 格式转换
```

---

## 10. Java 代码优化示例

见项目中的 `DuckDbMongoJoinOptimization.java` 文件，包含完整的实现示例。

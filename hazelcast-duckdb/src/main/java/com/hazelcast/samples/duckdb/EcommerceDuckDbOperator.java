/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.samples.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class EcommerceDuckDbOperator implements DuckDbOperator, AutoCloseable {

    private static final Object DB_LOCK = new Object();

    private final Connection connection;
    
    // 预编译语句复用，避免重复解析SQL
    private final PreparedStatement wideQuery;
    private final PreparedStatement deleteOrderMain;
    private final PreparedStatement deleteOrderItem;
    private final PreparedStatement upsertBuyer;
    private final PreparedStatement upsertOrderMain;
    private final PreparedStatement upsertOrderItem;

    // ==================== 攒批写入相关 ====================
    
    /**
     * 是否启用攒批写入模式
     */
    private final boolean batchWritingEnabled;
    
    /**
     * 攒批大小
     */
    private final int batchWritingSize;
    
    /**
     * 攒批超时时间（毫秒）
     */
    private final long batchWritingTimeoutMs;
    
    /**
     * 批次缓冲区（线程安全队列）
     */
    private final Queue<EcommerceOrderBatch> batchBuffer;
    
    /**
     * 上次刷新时间
     */
    private final AtomicLong lastFlushTime;
    
    /**
     * 定时刷新调度器
     */
    private final ScheduledExecutorService flushScheduler;
    
    /**
     * 累积的宽表查询结果（攒批模式下需要暂存）
     */
    private final Queue<Map<String, Object>> accumulatedResults;
    
    /**
     * 是否已关闭（volatile保证线程可见性）
     */
    private volatile boolean closed = false;

    public EcommerceDuckDbOperator() throws SQLException {
        connection = DriverManager.getConnection(PerfConfig.DUCKDB_JDBC_URL);
        connection.setAutoCommit(false);
        initializeSchema();
        
        // 预编译所有SQL语句，复用提高性能
        wideQuery = prepareWideQuery();
        deleteOrderMain = connection.prepareStatement("DELETE FROM order_main WHERE order_id = ?");
        deleteOrderItem = connection.prepareStatement("DELETE FROM order_item WHERE order_id = ?");
        upsertBuyer = prepareUpsertBuyer();
        upsertOrderMain = prepareUpsertOrderMain();
        upsertOrderItem = prepareUpsertOrderItem();
        
        // 初始化攒批相关配置
        this.batchWritingEnabled = PerfConfig.BATCH_WRITING_ENABLED;
        this.batchWritingSize = PerfConfig.BATCH_WRITING_SIZE;
        this.batchWritingTimeoutMs = PerfConfig.BATCH_WRITING_TIMEOUT_MS;
        this.batchBuffer = new LinkedList<>();
        this.lastFlushTime = new AtomicLong(System.currentTimeMillis());
        this.accumulatedResults = new LinkedList<>();
        
        // 如果启用攒批模式，启动定时刷新任务
        if (batchWritingEnabled) {
            this.flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "DuckDbFlushScheduler");
                thread.setDaemon(true);
                return thread;
            });
            // 每 200ms 检查一次是否需要刷新
            this.flushScheduler.scheduleAtFixedRate(this::flushIfTimeout, 0, 200, TimeUnit.MILLISECONDS);
        } else {
            this.flushScheduler = null;
        }
    }

    private void initializeSchema() throws SQLException {
        synchronized (DB_LOCK) {
            int maxAttempts = PerfConfig.MAX_TRANSACTION_RETRY;
            int attempt = 0;
            while (true) {
                attempt++;
                try (Statement statement = connection.createStatement()) {
                    executeSchema(statement);
                    connection.commit();
                    return;
                } catch (SQLException e) {
                    String message = e.getMessage() == null ? "" : e.getMessage();
                    if ((message.contains("Catalog write-write conflict")
                            || message.contains("Conflict on update")
                            || message.contains("Current transaction is aborted"))
                            && attempt < maxAttempts) {
                        try {
                            connection.rollback();
                        } catch (SQLException ignored) {
                            // ignore
                        }
                        try {
                            Thread.sleep(PerfConfig.RETRY_DELAY_BASE_MS * attempt);
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        continue;
                    }
                    throw e;
                }
            }
        }
    }

    private void executeSchema(Statement statement) throws SQLException {
        statement.execute("CREATE TABLE IF NOT EXISTS buyer_info (\n"
                + "    buyer_id BIGINT PRIMARY KEY,\n"
                + "    buyer_nickname VARCHAR,\n"
                + "    buyer_real_name VARCHAR,\n"
                + "    buyer_phone VARCHAR,\n"
                + "    buyer_level VARCHAR,\n"
                + "    register_area VARCHAR,\n"
                + "    register_time BIGINT\n"
                + ")");
        statement.execute("CREATE TABLE IF NOT EXISTS order_main (\n"
                + "    order_id BIGINT PRIMARY KEY,\n"
                + "    order_no VARCHAR,\n"
                + "    buyer_id BIGINT,\n"
                + "    create_time BIGINT,\n"
                + "    pay_time BIGINT,\n"
                + "    order_status VARCHAR,\n"
                + "    pay_way VARCHAR,\n"
                + "    order_channel VARCHAR,\n"
                + "    total_amount DOUBLE,\n"
                + "    pay_amount DOUBLE,\n"
                + "    freight_amount DOUBLE,\n"
                + "    coupon_amount DOUBLE,\n"
                + "    receiver_name VARCHAR,\n"
                + "    receiver_phone VARCHAR,\n"
                + "    receiver_address VARCHAR\n"
                + ")");
        statement.execute("CREATE TABLE IF NOT EXISTS order_item (\n"
                + "    item_id BIGINT PRIMARY KEY,\n"
                + "    order_id BIGINT,\n"
                + "    spu_no VARCHAR,\n"
                + "    sku_no VARCHAR,\n"
                + "    goods_name VARCHAR,\n"
                + "    category1 VARCHAR,\n"
                + "    category2 VARCHAR,\n"
                + "    brand_name VARCHAR,\n"
                + "    original_price DOUBLE,\n"
                + "    sale_price DOUBLE,\n"
                + "    buy_num INT,\n"
                + "    item_subtotal DOUBLE,\n"
                + "    goods_spec VARCHAR\n"
                + ")");
        // 创建索引优化JOIN查询性能
        // order_item表：order_id用于JOIN查询
        statement.execute("CREATE INDEX IF NOT EXISTS idx_order_item_order_id ON order_item(order_id)");
        // order_main表：buyer_id用于JOIN查询
        statement.execute("CREATE INDEX IF NOT EXISTS idx_order_main_buyer_id ON order_main(buyer_id)");
        // order_main表：order_id用于批量查询（IN子句）
        statement.execute("CREATE INDEX IF NOT EXISTS idx_order_main_order_id ON order_main(order_id)");
        // buyer_info表：buyer_id用于JOIN查询（主键已自动创建索引，此处显式创建确保存在）
        statement.execute("CREATE INDEX IF NOT EXISTS idx_buyer_info_buyer_id ON buyer_info(buyer_id)");
    }

    private PreparedStatement prepareWideQuery() throws SQLException {
        return connection.prepareStatement("SELECT\n"
                + "    bi.buyer_id,\n"
                + "    bi.buyer_nickname,\n"
                + "    bi.buyer_real_name,\n"
                + "    bi.buyer_phone,\n"
                + "    bi.buyer_level,\n"
                + "    bi.register_area,\n"
                + "    bi.register_time,\n"
                + "    om.order_id,\n"
                + "    om.order_no,\n"
                + "    om.create_time,\n"
                + "    om.pay_time,\n"
                + "    om.order_status,\n"
                + "    om.pay_way,\n"
                + "    om.order_channel,\n"
                + "    om.total_amount,\n"
                + "    om.pay_amount,\n"
                + "    om.freight_amount,\n"
                + "    om.coupon_amount,\n"
                + "    om.receiver_name,\n"
                + "    om.receiver_phone,\n"
                + "    om.receiver_address,\n"
                + "    oi.item_id,\n"
                + "    oi.spu_no,\n"
                + "    oi.sku_no,\n"
                + "    oi.goods_name,\n"
                + "    oi.category1,\n"
                + "    oi.category2,\n"
                + "    oi.brand_name,\n"
                + "    oi.original_price,\n"
                + "    oi.sale_price,\n"
                + "    oi.buy_num,\n"
                + "    oi.item_subtotal,\n"
                + "    oi.goods_spec\n"
                + "FROM order_main om\n"
                + "LEFT JOIN buyer_info bi ON om.buyer_id = bi.buyer_id\n"
                + "LEFT JOIN order_item oi ON om.order_id = oi.order_id\n"
                + "WHERE om.order_id = ?\n");
    }

    private PreparedStatement prepareUpsertBuyer() throws SQLException {
        return connection.prepareStatement("INSERT OR REPLACE INTO buyer_info (\n"
                + "    buyer_id, buyer_nickname, buyer_real_name, buyer_phone, buyer_level, register_area, register_time\n"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?)\n");
    }

    private PreparedStatement prepareUpsertOrderMain() throws SQLException {
        return connection.prepareStatement("INSERT OR REPLACE INTO order_main (\n"
                + "    order_id, order_no, buyer_id, create_time, pay_time, order_status, pay_way, order_channel,\n"
                + "    total_amount, pay_amount, freight_amount, coupon_amount, receiver_name, receiver_phone,\n"
                + "    receiver_address\n"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n");
    }

    private PreparedStatement prepareUpsertOrderItem() throws SQLException {
        return connection.prepareStatement("INSERT OR REPLACE INTO order_item (\n"
                + "    item_id, order_id, spu_no, sku_no, goods_name, category1, category2, brand_name,\n"
                + "    original_price, sale_price, buy_num, item_subtotal, goods_spec\n"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n");
    }

    /**
     * 处理批次数据（支持攒批模式）
     * 
     * @param batch 批次数据
     * @return 宽表查询结果（攒批模式下可能返回空列表，结果会累积到后续flush时返回）
     * @throws SQLException SQL异常
     */
    public List<Map<String, Object>> processBatch(EcommerceOrderBatch batch) throws SQLException {
        Objects.requireNonNull(batch, "batch");
        
        // 如果已关闭，抛出异常
        if (closed) {
            throw new SQLException("Operator has been closed");
        }
        
        if (batchWritingEnabled) {
            // 攒批模式：将批次加入缓冲区
            synchronized (batchBuffer) {
                // 双重检查关闭状态
                if (closed) {
                    throw new SQLException("Operator has been closed");
                }
                
                batchBuffer.offer(batch);
                
                // 检查是否达到攒批大小
                if (batchBuffer.size() >= batchWritingSize) {
                    // 达到阈值，执行批量写入（已持有锁，直接调用内部方法）
                    List<Map<String, Object>> results = flushInternalLocked();
                    // 返回累积的结果
                    List<Map<String, Object>> toReturn = new ArrayList<>();
                    synchronized (accumulatedResults) {
                        toReturn.addAll(accumulatedResults);
                        accumulatedResults.clear();
                    }
                    return toReturn;
                }
                
                // 未达到阈值，返回空列表（结果会在后续flush时返回）
                return List.of();
            }
        } else {
            // 非攒批模式：立即处理
            long startNanos = System.nanoTime();
            List<Map<String, Object>> rows = null;
            int maxAttempts = PerfConfig.MAX_TRANSACTION_RETRY;
            int attempt = 0;
            while (true) {
                attempt++;
                try {
                    rows = performTransaction(List.of(batch));
                    break;
                } catch (SQLException e) {
                    try {
                        connection.rollback();
                    } catch (SQLException ignored) {
                        // ignore
                    }
                    String message = e.getMessage() == null ? "" : e.getMessage();
                    if ((message.contains("Conflict on update") || message.contains("Current transaction is aborted"))
                            && attempt < maxAttempts) {
                        try {
                            Thread.sleep(PerfConfig.RETRY_DELAY_BASE_MS * attempt);
                        } catch (InterruptedException interruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        continue;
                    }
                    throw e;
                }
            }
            long durationNanos = System.nanoTime() - startNanos;
            StatsCollector.getInstance().recordBatch(1, rows.size(), durationNanos);
            return rows;
        }
    }

    /**
     * 获取当前累积的结果（用于流处理场景下的结果输出）
     * 
     * @return 累积的宽表查询结果
     */
    public List<Map<String, Object>> drainResults() {
        if (!batchWritingEnabled) {
            return List.of();
        }
        
        synchronized (accumulatedResults) {
            List<Map<String, Object>> results = new ArrayList<>(accumulatedResults);
            accumulatedResults.clear();
            return results;
        }
    }

    /**
     * 强制刷新缓冲区（同步调用）
     * 
     * @return 刷新后累积的宽表查询结果
     * @throws SQLException SQL异常
     */
    @Override
    public List<Map<String, Object>> flush() throws SQLException {
        if (!batchWritingEnabled) {
            return List.of();
        }
        
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return drainResults();
            }
            flushInternal();
            return drainResults();
        }
    }

    /**
     * 如果超时则刷新缓冲区（定时任务调用）
     */
    private void flushIfTimeout() {
        // 如果已关闭或未启用攒批模式，直接返回
        if (closed || !batchWritingEnabled) {
            return;
        }
        
        long now = System.currentTimeMillis();
        long last = lastFlushTime.get();
        
        synchronized (batchBuffer) {
            // 双重检查关闭状态（因为可能在获取锁期间被关闭）
            if (closed) {
                return;
            }
            if (!batchBuffer.isEmpty() && (now - last) >= batchWritingTimeoutMs) {
                try {
                    flushInternal();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 内部刷新方法，执行批量写入（已持有batchBuffer锁时调用）
     */
    private List<Map<String, Object>> flushInternalLocked() throws SQLException {
        if (batchBuffer.isEmpty()) {
            return List.of();
        }
        
        long startNanos = System.nanoTime();
        
        // 获取所有待处理的批次
        List<EcommerceOrderBatch> batchesToProcess = new ArrayList<>();
        while (!batchBuffer.isEmpty()) {
            batchesToProcess.add(batchBuffer.poll());
        }
        lastFlushTime.set(System.currentTimeMillis());
        
        // 执行批量事务
        List<Map<String, Object>> allResults = null;
        int maxAttempts = PerfConfig.MAX_TRANSACTION_RETRY;
        int attempt = 0;
        while (true) {
            attempt++;
            // 在每次重试前检查是否已关闭
            if (closed) {
                throw new SQLException("Operator has been closed during transaction");
            }
            try {
                allResults = performTransaction(batchesToProcess);
                break;
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException ignored) {
                    // ignore
                }
                String message = e.getMessage() == null ? "" : e.getMessage();
                // 只有事务冲突类异常才重试，Statement was closed 等致命异常直接抛出
                if ((message.contains("Conflict on update") || message.contains("Current transaction is aborted"))
                        && attempt < maxAttempts) {
                    try {
                        Thread.sleep(PerfConfig.RETRY_DELAY_BASE_MS * attempt);
                    } catch (InterruptedException interruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }
                throw e;
            }
        }
        
        // 记录统计
        long durationNanos = System.nanoTime() - startNanos;
        int totalRows = allResults.size();
        StatsCollector.getInstance().recordBatch(batchesToProcess.size(), totalRows, durationNanos);
        
        // 打印DuckDB性能统计（类似HighPerformanceDataGenerator的printStats）
        StatsCollector.getInstance().printStats();
        
        // 将结果累积到结果缓冲区
        synchronized (accumulatedResults) {
            accumulatedResults.addAll(allResults);
        }
        
        return allResults;
    }
    
    /**
     * 内部刷新方法，执行批量写入
     */
    private List<Map<String, Object>> flushInternal() throws SQLException {
        // 如果已关闭，直接返回空列表
        if (closed) {
            return List.of();
        }
        
        // 在锁内执行整个批量写入操作，防止在写入过程中被关闭
        synchronized (batchBuffer) {
            // 再次检查关闭状态
            if (closed) {
                return List.of();
            }
            
            return flushInternalLocked();
        }
    }

    /**
     * 执行批量事务处理
     * 
     * @param batches 批次列表
     * @return 所有批次的宽表查询结果
     */
    private List<Map<String, Object>> performTransaction(List<EcommerceOrderBatch> batches) throws SQLException {
        List<Map<String, Object>> allResults = new ArrayList<>();
        
        synchronized (DB_LOCK) {
            long buyerStartNanos = System.nanoTime();
            int buyerCount = 0;
            
            long orderStartNanos = System.nanoTime();
            int orderCount = 0;
            
            long itemStartNanos = System.nanoTime();
            int itemCount = 0;
            
            long joinStartNanos = System.nanoTime();
            int joinCount = 0;
            
            // 先批量处理所有删除操作
            for (EcommerceOrderBatch batch : batches) {
                for (Long orderId : batch.deleteOrderIds()) {
                    cascadeDeleteOrder(orderId.longValue());
                }
            }
            
            // 批量 upsert buyer_info（使用 addBatch）
            for (EcommerceOrderBatch batch : batches) {
                if (batch.buyer() != null) {
                    upsertBuyer.setLong(1, batch.buyer().buyerId());
                    upsertBuyer.setString(2, batch.buyer().buyerNickname());
                    upsertBuyer.setString(3, batch.buyer().buyerRealName());
                    upsertBuyer.setString(4, batch.buyer().buyerPhone());
                    upsertBuyer.setString(5, batch.buyer().buyerLevel());
                    upsertBuyer.setString(6, batch.buyer().registerArea());
                    upsertBuyer.setLong(7, batch.buyer().registerTime());
                    upsertBuyer.addBatch();
                    buyerCount++;
                }
            }
            if (buyerCount > 0) {
                upsertBuyer.executeBatch();
                upsertBuyer.clearBatch();
            }
            StatsCollector.getInstance().recordTableWrite("buyer_info", buyerCount, System.nanoTime() - buyerStartNanos);
            
            // 批量 upsert order_main（使用 addBatch）
            for (EcommerceOrderBatch batch : batches) {
                if (batch.order() != null) {
                    EcommerceOrder order = batch.order();
                    upsertOrderMain.setLong(1, order.orderId());
                    upsertOrderMain.setString(2, order.orderNo());
                    upsertOrderMain.setLong(3, order.buyerId());
                    upsertOrderMain.setLong(4, order.createTime());
                    upsertOrderMain.setLong(5, order.payTime());
                    upsertOrderMain.setString(6, order.orderStatus());
                    upsertOrderMain.setString(7, order.payWay());
                    upsertOrderMain.setString(8, order.orderChannel());
                    upsertOrderMain.setDouble(9, order.totalAmount());
                    upsertOrderMain.setDouble(10, order.payAmount());
                    upsertOrderMain.setDouble(11, order.freightAmount());
                    upsertOrderMain.setDouble(12, order.couponAmount());
                    upsertOrderMain.setString(13, order.receiverName());
                    upsertOrderMain.setString(14, order.receiverPhone());
                    upsertOrderMain.setString(15, order.receiverAddress());
                    upsertOrderMain.addBatch();
                    orderCount++;
                }
            }
            if (orderCount > 0) {
                upsertOrderMain.executeBatch();
                upsertOrderMain.clearBatch();
            }
            StatsCollector.getInstance().recordTableWrite("order_main", orderCount, System.nanoTime() - orderStartNanos);
            
            // 批量 upsert order_item（使用 addBatch）
            for (EcommerceOrderBatch batch : batches) {
                for (EcommerceOrderItem item : batch.items()) {
                    upsertOrderItem.setLong(1, item.itemId());
                    upsertOrderItem.setLong(2, item.orderId());
                    upsertOrderItem.setString(3, item.spuNo());
                    upsertOrderItem.setString(4, item.skuNo());
                    upsertOrderItem.setString(5, item.goodsName());
                    upsertOrderItem.setString(6, item.category1());
                    upsertOrderItem.setString(7, item.category2());
                    upsertOrderItem.setString(8, item.brandName());
                    upsertOrderItem.setDouble(9, item.originalPrice());
                    upsertOrderItem.setDouble(10, item.salePrice());
                    upsertOrderItem.setInt(11, item.buyNum());
                    upsertOrderItem.setDouble(12, item.itemSubtotal());
                    upsertOrderItem.setString(13, item.goodsSpec());
                    upsertOrderItem.addBatch();
                    itemCount++;
                }
            }
            if (itemCount > 0) {
                upsertOrderItem.executeBatch();
                upsertOrderItem.clearBatch();
            }
            StatsCollector.getInstance().recordTableWrite("order_item", itemCount, System.nanoTime() - itemStartNanos);
            
            // 提交事务
            connection.commit();
            
            // 执行宽表查询（批量查询，一次性查询所有orderId）
            List<Long> orderIds = new ArrayList<>();
            for (EcommerceOrderBatch batch : batches) {
                if (batch.order() != null) {
                    orderIds.add(batch.order().orderId());
                }
            }
            if (!orderIds.isEmpty()) {
                List<Map<String, Object>> rows = queryWideRowsBatch(orderIds);
                allResults.addAll(rows);
                joinCount += rows.size();
            }
            StatsCollector.getInstance().recordJoin(joinCount, System.nanoTime() - joinStartNanos);
        }
        
        return allResults;
    }

    private void cascadeDeleteOrder(long orderId) throws SQLException {
        deleteOrderItem.setLong(1, orderId);
        deleteOrderItem.executeUpdate();
        deleteOrderMain.setLong(1, orderId);
        deleteOrderMain.executeUpdate();
    }

    List<Map<String, Object>> queryWideRows(long orderId) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        wideQuery.setLong(1, orderId);
        try (ResultSet resultSet = wideQuery.executeQuery()) {
            while (resultSet.next()) {
                rows.add(toWideRow(resultSet));
            }
        }
        return rows;
    }

    /**
     * 批量查询宽表数据（一次查询多个orderId）
     * 
     * @param orderIds orderId集合
     * @return 宽表查询结果
     * @throws SQLException SQL异常
     */
    List<Map<String, Object>> queryWideRowsBatch(List<Long> orderIds) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        
        if (orderIds.isEmpty()) {
            return rows;
        }
        
        // 构建IN查询SQL
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT\n");
        sqlBuilder.append("    bi.buyer_id,\n");
        sqlBuilder.append("    bi.buyer_nickname,\n");
        sqlBuilder.append("    bi.buyer_real_name,\n");
        sqlBuilder.append("    bi.buyer_phone,\n");
        sqlBuilder.append("    bi.buyer_level,\n");
        sqlBuilder.append("    bi.register_area,\n");
        sqlBuilder.append("    bi.register_time,\n");
        sqlBuilder.append("    om.order_id,\n");
        sqlBuilder.append("    om.order_no,\n");
        sqlBuilder.append("    om.create_time,\n");
        sqlBuilder.append("    om.pay_time,\n");
        sqlBuilder.append("    om.order_status,\n");
        sqlBuilder.append("    om.pay_way,\n");
        sqlBuilder.append("    om.order_channel,\n");
        sqlBuilder.append("    om.total_amount,\n");
        sqlBuilder.append("    om.pay_amount,\n");
        sqlBuilder.append("    om.freight_amount,\n");
        sqlBuilder.append("    om.coupon_amount,\n");
        sqlBuilder.append("    om.receiver_name,\n");
        sqlBuilder.append("    om.receiver_phone,\n");
        sqlBuilder.append("    om.receiver_address,\n");
        sqlBuilder.append("    oi.item_id,\n");
        sqlBuilder.append("    oi.spu_no,\n");
        sqlBuilder.append("    oi.sku_no,\n");
        sqlBuilder.append("    oi.goods_name,\n");
        sqlBuilder.append("    oi.category1,\n");
        sqlBuilder.append("    oi.category2,\n");
        sqlBuilder.append("    oi.brand_name,\n");
        sqlBuilder.append("    oi.original_price,\n");
        sqlBuilder.append("    oi.sale_price,\n");
        sqlBuilder.append("    oi.buy_num,\n");
        sqlBuilder.append("    oi.item_subtotal,\n");
        sqlBuilder.append("    oi.goods_spec\n");
        sqlBuilder.append("FROM order_main om\n");
        sqlBuilder.append("LEFT JOIN buyer_info bi ON om.buyer_id = bi.buyer_id\n");
        sqlBuilder.append("LEFT JOIN order_item oi ON om.order_id = oi.order_id\n");
        sqlBuilder.append("WHERE om.order_id IN (");
        
        // 添加orderId参数占位符
        for (int i = 0; i < orderIds.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append("?");
        }
        sqlBuilder.append(")\n");
        
        // 预编译并执行批量查询
        try (PreparedStatement batchQuery = connection.prepareStatement(sqlBuilder.toString())) {
            // 设置参数
            for (int i = 0; i < orderIds.size(); i++) {
                batchQuery.setLong(i + 1, orderIds.get(i));
            }
            
            // 执行查询
            try (ResultSet resultSet = batchQuery.executeQuery()) {
                while (resultSet.next()) {
                    rows.add(toWideRow(resultSet));
                }
            }
        }
        
        return rows;
    }

    private Map<String, Object> toWideRow(ResultSet resultSet) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("buyer_id", resultSet.getLong("buyer_id"));
        row.put("buyer_nickname", resultSet.getString("buyer_nickname"));
        row.put("buyer_real_name", resultSet.getString("buyer_real_name"));
        row.put("buyer_phone", resultSet.getString("buyer_phone"));
        row.put("buyer_level", resultSet.getString("buyer_level"));
        row.put("register_area", resultSet.getString("register_area"));
        row.put("register_time", resultSet.getLong("register_time"));
        row.put("order_id", resultSet.getLong("order_id"));
        row.put("order_no", resultSet.getString("order_no"));
        row.put("create_time", resultSet.getLong("create_time"));
        row.put("pay_time", resultSet.getLong("pay_time"));
        row.put("order_status", resultSet.getString("order_status"));
        row.put("pay_way", resultSet.getString("pay_way"));
        row.put("order_channel", resultSet.getString("order_channel"));
        row.put("total_amount", resultSet.getDouble("total_amount"));
        row.put("pay_amount", resultSet.getDouble("pay_amount"));
        row.put("freight_amount", resultSet.getDouble("freight_amount"));
        row.put("coupon_amount", resultSet.getDouble("coupon_amount"));
        row.put("receiver_name", resultSet.getString("receiver_name"));
        row.put("receiver_phone", resultSet.getString("receiver_phone"));
        row.put("receiver_address", resultSet.getString("receiver_address"));
        row.put("item_id", resultSet.getLong("item_id"));
        row.put("spu_no", resultSet.getString("spu_no"));
        row.put("sku_no", resultSet.getString("sku_no"));
        row.put("goods_name", resultSet.getString("goods_name"));
        row.put("category1", resultSet.getString("category1"));
        row.put("category2", resultSet.getString("category2"));
        row.put("brand_name", resultSet.getString("brand_name"));
        row.put("original_price", resultSet.getDouble("original_price"));
        row.put("sale_price", resultSet.getDouble("sale_price"));
        row.put("buy_num", resultSet.getInt("buy_num"));
        row.put("item_subtotal", resultSet.getDouble("item_subtotal"));
        row.put("goods_spec", resultSet.getString("goods_spec"));
        return row;
    }

    @Override
    public void close() throws Exception {
        // 先获取锁，等待正在进行的写入操作完成
        synchronized (batchBuffer) {
            // 设置关闭标志（在锁内设置，确保不会有新的写入操作开始）
            closed = true;
            
            // 刷新剩余数据（在锁内执行，确保不会被其他线程打断）
            try {
                if (batchWritingEnabled && !batchBuffer.isEmpty()) {
                    flushInternalLocked();
                }
            } catch (SQLException e) {
                // ignore flush errors during close
            }
        }
        
        // 停止定时刷新任务并等待完成
        if (flushScheduler != null) {
            flushScheduler.shutdown();
            try {
                // 等待最多5秒让定时任务完成
                if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    flushScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                flushScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // ignore
        }
        
        // 关闭所有预编译语句
        wideQuery.close();
        deleteOrderMain.close();
        deleteOrderItem.close();
        upsertBuyer.close();
        upsertOrderMain.close();
        upsertOrderItem.close();
        connection.close();
    }
}
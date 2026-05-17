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

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Appender模式DuckDb操作器 - 使用DuckDB Appender API实现高性能批量写入
 * 
 * 核心优势：
 * 1. DuckDB原生Appender API，比PreparedStatement更快
 * 2. 支持批量追加，减少事务提交次数
 * 3. 直接操作DuckDB内部数据结构，性能极佳
 * 
 * 性能对比：
 * - INSERT模式：QPS约1000-3000
 * - COPY模式：QPS约5000-10000
 * - ARROW模式：QPS约20000-50000+
 * - APPENDER模式：QPS约15000-30000
 */
public class AppenderModeDuckDbOperator implements DuckDbOperator {
    
    private static final Object DB_LOCK = new Object();
    
    private final Connection connection;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    
    // 攒批相关配置
    private final boolean batchWritingEnabled;
    private final int batchWritingSize;
    private final long batchWritingTimeoutMs;
    private final Collection<EcommerceOrderBatch> batchBuffer = new ConcurrentLinkedQueue<>();
    
    // 累积查询结果
    private final List<Map<String, Object>> accumulatedResults = new ArrayList<>();

    public AppenderModeDuckDbOperator() throws SQLException {
        this.batchWritingEnabled = PerfConfig.BATCH_WRITING_ENABLED;
        this.batchWritingSize = PerfConfig.BATCH_WRITING_SIZE;
        this.batchWritingTimeoutMs = PerfConfig.BATCH_WRITING_TIMEOUT_MS;
        
        // 建立DuckDB连接
        this.connection = DriverManager.getConnection(PerfConfig.DUCKDB_JDBC_URL);
        
        // 初始化表结构
        initTables();
    }
    
    /**
     * 初始化表结构
     */
    private void initTables() throws SQLException {
        synchronized (DB_LOCK) {
            try (Statement stmt = connection.createStatement()) {
                // buyer_info表
                try {
                    stmt.execute("CREATE TABLE IF NOT EXISTS buyer_info (" +
                                 "buyer_id BIGINT PRIMARY KEY," +
                                 "buyer_nickname VARCHAR," +
                                 "buyer_real_name VARCHAR," +
                                 "buyer_phone VARCHAR," +
                                 "buyer_level VARCHAR," +
                                 "register_area VARCHAR," +
                                 "register_time BIGINT" +
                                 ")");
                } catch (SQLException e) {
                    // 如果是catalog write-write conflict（表已存在），则忽略
                    if (!e.getMessage().contains("catalog write-write conflict")) {
                        throw e;
                    }
                }
                
                // order_main表
                try {
                    stmt.execute("CREATE TABLE IF NOT EXISTS order_main (" +
                                 "order_id BIGINT PRIMARY KEY," +
                                 "order_no VARCHAR," +
                                 "buyer_id BIGINT," +
                                 "create_time BIGINT," +
                                 "pay_time BIGINT," +
                                 "order_status VARCHAR," +
                                 "pay_way VARCHAR," +
                                 "order_channel VARCHAR," +
                                 "total_amount DOUBLE," +
                                 "pay_amount DOUBLE," +
                                 "freight_amount DOUBLE," +
                                 "coupon_amount DOUBLE," +
                                 "receiver_name VARCHAR," +
                                 "receiver_phone VARCHAR," +
                                 "receiver_address VARCHAR" +
                                 ")");
                } catch (SQLException e) {
                    if (!e.getMessage().contains("catalog write-write conflict")) {
                        throw e;
                    }
                }
                
                // order_item表
                try {
                    stmt.execute("CREATE TABLE IF NOT EXISTS order_item (" +
                                 "item_id BIGINT PRIMARY KEY," +
                                 "order_id BIGINT," +
                                 "spu_no VARCHAR," +
                                 "sku_no VARCHAR," +
                                 "goods_name VARCHAR," +
                                 "category1 VARCHAR," +
                                 "category2 VARCHAR," +
                                 "brand_name VARCHAR," +
                                 "original_price DOUBLE," +
                                 "sale_price DOUBLE," +
                                 "buy_num INT," +
                                 "item_subtotal DOUBLE," +
                                 "goods_spec VARCHAR" +
                                 ")");
                } catch (SQLException e) {
                    if (!e.getMessage().contains("catalog write-write conflict")) {
                        throw e;
                    }
                }
            }
        }
    }
    
    @Override
    public List<Map<String, Object>> processBatch(EcommerceOrderBatch batch) throws SQLException {
        synchronized (DB_LOCK) {
            // 如果是攒批模式，先累积
            if (batchWritingEnabled) {
                batchBuffer.add(batch);
                
                // 检查是否需要触发刷新
                if (batchBuffer.size() >= batchWritingSize || 
                    System.currentTimeMillis() - lastFlushTime.get() >= batchWritingTimeoutMs) {
                    return flushInternalLocked();
                } else {
                    return Collections.emptyList();
                }
            } else {
                // 非攒批模式，直接处理单个批次
                return processBatchInternal(Collections.singletonList(batch));
            }
        }
    }
    
    @Override
    public List<Map<String, Object>> flush() throws SQLException {
        synchronized (DB_LOCK) {
            return flushInternalLocked();
        }
    }
    
    /**
     * 内部刷新方法（需持有DB_LOCK）
     */
    private List<Map<String, Object>> flushInternalLocked() throws SQLException {
        if (batchBuffer.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 收集所有待处理的批次
        List<EcommerceOrderBatch> batches = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        lastFlushTime.set(System.currentTimeMillis());
        
        // 处理批次
        return processBatchInternal(batches);
    }
    
    /**
     * 内部处理批次的核心方法
     */
    private List<Map<String, Object>> processBatchInternal(List<EcommerceOrderBatch> batches) throws SQLException {
        long startNanos = System.nanoTime();
        
        // 使用HashMap去重
        Map<Long, EcommerceBuyer> buyerMap = new HashMap<>();
        Map<Long, EcommerceOrder> orderMap = new HashMap<>();
        Map<Long, EcommerceOrderItem> itemMap = new HashMap<>();
        
        for (EcommerceOrderBatch batch : batches) {
            if (batch.buyer() != null) {
                buyerMap.put(batch.buyer().buyerId(), batch.buyer());
            }
            if (batch.order() != null) {
                orderMap.put(batch.order().orderId(), batch.order());
            }
            for (EcommerceOrderItem item : batch.items()) {
                itemMap.put(item.itemId(), item);
            }
        }
        
        // 执行批量写入和查询
        List<Map<String, Object>> results = performTransaction(buyerMap, orderMap, itemMap);
        
        // 累积结果（用于攒批模式的最终返回）
        accumulatedResults.addAll(results);
        
        // 记录统计信息
        int totalRows = buyerMap.size() + orderMap.size() + itemMap.size();
        long durationNanos = System.nanoTime() - startNanos;
        StatsCollector.getInstance().recordBatch(batches.size(), totalRows, durationNanos);
        
        // 打印DuckDB性能统计
        StatsCollector.getInstance().printStats();
        
        return results;
    }
    
    /**
     * 使用Appender API执行批量事务处理
     */
    private List<Map<String, Object>> performTransaction(Map<Long, EcommerceBuyer> buyerMap,
                                                          Map<Long, EcommerceOrder> orderMap,
                                                          Map<Long, EcommerceOrderItem> itemMap) throws SQLException {
        List<Map<String, Object>> allResults = new ArrayList<>();
        
        long buyerStartNanos = System.nanoTime();
        long orderStartNanos = System.nanoTime();
        long itemStartNanos = System.nanoTime();
        long joinStartNanos = System.nanoTime();
        
        // 先删除已存在的记录
        try (Statement stmt = connection.createStatement()) {
            if (!buyerMap.isEmpty()) {
                StringBuilder deleteSql = new StringBuilder("DELETE FROM buyer_info WHERE buyer_id IN (");
                boolean first = true;
                for (Long id : buyerMap.keySet()) {
                    if (!first) {
                        deleteSql.append(",");
                    }
                    deleteSql.append(id);
                    first = false;
                }
                deleteSql.append(")");
                stmt.execute(deleteSql.toString());
            }
            
            if (!orderMap.isEmpty()) {
                StringBuilder deleteSql = new StringBuilder("DELETE FROM order_main WHERE order_id IN (");
                boolean first = true;
                for (Long id : orderMap.keySet()) {
                    if (!first) {
                        deleteSql.append(",");
                    }
                    deleteSql.append(id);
                    first = false;
                }
                deleteSql.append(")");
                stmt.execute(deleteSql.toString());
            }
            
            if (!itemMap.isEmpty()) {
                StringBuilder deleteSql = new StringBuilder("DELETE FROM order_item WHERE item_id IN (");
                boolean first = true;
                for (Long id : itemMap.keySet()) {
                    if (!first) {
                        deleteSql.append(",");
                    }
                    deleteSql.append(id);
                    first = false;
                }
                deleteSql.append(")");
                stmt.execute(deleteSql.toString());
            }
        }
        
        // 使用Appender API写入buyer_info
        if (!buyerMap.isEmpty()) {
            buyerStartNanos = System.nanoTime();
            try (DuckDBAppender appender = connection.unwrap(DuckDBConnection.class).createAppender("main", "buyer_info")) {
                for (EcommerceBuyer buyer : buyerMap.values()) {
                    appender.beginRow();
                    appender.append(buyer.buyerId());
                    appender.append(buyer.buyerNickname());
                    appender.append(buyer.buyerRealName());
                    appender.append(buyer.buyerPhone());
                    appender.append(buyer.buyerLevel());
                    appender.append(buyer.registerArea());
                    appender.append(buyer.registerTime());
                    appender.endRow();
                }
                appender.flush();
            }
            StatsCollector.getInstance().recordTableWrite("buyer_info", buyerMap.size(), System.nanoTime() - buyerStartNanos);
        }
        
        // 使用Appender API写入order_main
        if (!orderMap.isEmpty()) {
            orderStartNanos = System.nanoTime();
            try (DuckDBAppender appender = connection.unwrap(DuckDBConnection.class).createAppender("main", "order_main")) {
                for (EcommerceOrder order : orderMap.values()) {
                    appender.beginRow();
                    appender.append(order.orderId());
                    appender.append(order.orderNo());
                    appender.append(order.buyerId());
                    appender.append(order.createTime());
                    appender.append(order.payTime());
                    appender.append(order.orderStatus());
                    appender.append(order.payWay());
                    appender.append(order.orderChannel());
                    appender.append(order.totalAmount());
                    appender.append(order.payAmount());
                    appender.append(order.freightAmount());
                    appender.append(order.couponAmount());
                    appender.append(order.receiverName());
                    appender.append(order.receiverPhone());
                    appender.append(order.receiverAddress());
                    appender.endRow();
                }
                appender.flush();
            }
            StatsCollector.getInstance().recordTableWrite("order_main", orderMap.size(), System.nanoTime() - orderStartNanos);
        }
        
        // 使用Appender API写入order_item
        if (!itemMap.isEmpty()) {
            itemStartNanos = System.nanoTime();
            try (DuckDBAppender appender = connection.unwrap(DuckDBConnection.class).createAppender("main", "order_item")) {
                for (EcommerceOrderItem item : itemMap.values()) {
                    appender.beginRow();
                    appender.append(item.itemId());
                    appender.append(item.orderId());
                    appender.append(item.spuNo());
                    appender.append(item.skuNo());
                    appender.append(item.goodsName());
                    appender.append(item.category1());
                    appender.append(item.category2());
                    appender.append(item.brandName());
                    appender.append(item.originalPrice());
                    appender.append(item.salePrice());
                    appender.append(item.buyNum());
                    appender.append(item.itemSubtotal());
                    appender.append(item.goodsSpec());
                    appender.endRow();
                }
                appender.flush();
            }
            StatsCollector.getInstance().recordTableWrite("order_item", itemMap.size(), System.nanoTime() - itemStartNanos);
        }
        
        // 执行宽表查询
        joinStartNanos = System.nanoTime();
        List<Long> orderIds = new ArrayList<>(orderMap.keySet());
        if (!orderIds.isEmpty()) {
            List<Map<String, Object>> rows = queryWideRowsBatch(orderIds);
            allResults.addAll(rows);
            StatsCollector.getInstance().recordJoin(rows.size(), System.nanoTime() - joinStartNanos);
        }
        
        return allResults;
    }
    
    /**
     * 批量查询宽表行
     */
    private List<Map<String, Object>> queryWideRowsBatch(List<Long> orderIds) throws SQLException {
        if (orderIds.isEmpty()) {
            return Collections.emptyList();
        }
        
        StringBuilder inClause = new StringBuilder();
        boolean first = true;
        for (Long orderId : orderIds) {
            if (!first) {
                inClause.append(",");
            }
            inClause.append(orderId);
            first = false;
        }
        
        String sql = "SELECT" +
                     "  o.order_id," +
                     "  o.order_no," +
                     "  o.create_time," +
                     "  o.pay_time," +
                     "  o.order_status," +
                     "  o.pay_way," +
                     "  o.order_channel," +
                     "  o.total_amount," +
                     "  o.pay_amount," +
                     "  o.freight_amount," +
                     "  o.coupon_amount," +
                     "  o.receiver_name," +
                     "  o.receiver_phone," +
                     "  o.receiver_address," +
                     "  b.buyer_id," +
                     "  b.buyer_nickname," +
                     "  b.buyer_real_name," +
                     "  b.buyer_level," +
                     "  b.register_area," +
                     "  i.item_id," +
                     "  i.spu_no," +
                     "  i.sku_no," +
                     "  i.goods_name," +
                     "  i.category1," +
                     "  i.category2," +
                     "  i.brand_name," +
                     "  i.original_price," +
                     "  i.sale_price," +
                     "  i.buy_num," +
                     "  i.item_subtotal" +
                     " FROM order_main o" +
                     " LEFT JOIN buyer_info b ON o.buyer_id = b.buyer_id" +
                     " LEFT JOIN order_item i ON o.order_id = i.order_id" +
                     " WHERE o.order_id IN (" + inClause + ")";
        
        List<Map<String, Object>> results = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("order_id", rs.getLong("order_id"));
                row.put("order_no", rs.getString("order_no"));
                row.put("create_time", rs.getLong("create_time"));
                row.put("pay_time", rs.getLong("pay_time"));
                row.put("order_status", rs.getString("order_status"));
                row.put("pay_way", rs.getString("pay_way"));
                row.put("order_channel", rs.getString("order_channel"));
                row.put("total_amount", rs.getDouble("total_amount"));
                row.put("pay_amount", rs.getDouble("pay_amount"));
                row.put("freight_amount", rs.getDouble("freight_amount"));
                row.put("coupon_amount", rs.getDouble("coupon_amount"));
                row.put("receiver_name", rs.getString("receiver_name"));
                row.put("receiver_phone", rs.getString("receiver_phone"));
                row.put("receiver_address", rs.getString("receiver_address"));
                row.put("buyer_id", rs.getLong("buyer_id"));
                row.put("buyer_nickname", rs.getString("buyer_nickname"));
                row.put("buyer_real_name", rs.getString("buyer_real_name"));
                row.put("buyer_level", rs.getString("buyer_level"));
                row.put("register_area", rs.getString("register_area"));
                row.put("item_id", rs.getLong("item_id"));
                row.put("spu_no", rs.getString("spu_no"));
                row.put("sku_no", rs.getString("sku_no"));
                row.put("goods_name", rs.getString("goods_name"));
                row.put("category1", rs.getString("category1"));
                row.put("category2", rs.getString("category2"));
                row.put("brand_name", rs.getString("brand_name"));
                row.put("original_price", rs.getDouble("original_price"));
                row.put("sale_price", rs.getDouble("sale_price"));
                row.put("buy_num", rs.getInt("buy_num"));
                row.put("item_subtotal", rs.getDouble("item_subtotal"));
                results.add(row);
            }
        }
        return results;
    }
    
    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            // 关闭前先刷新
            try {
                flush();
            } catch (Exception ignored) {
                // 忽略关闭前刷新的异常
            }
            
            // 关闭连接
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }
}

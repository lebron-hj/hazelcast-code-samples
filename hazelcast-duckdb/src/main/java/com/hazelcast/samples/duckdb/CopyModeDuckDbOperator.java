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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * COPY模式DuckDB操作器 - 使用COPY FROM命令实现高性能批量写入
 * 
 * COPY命令是DuckDB中最高效的批量导入方式，比PreparedStatement快10-100倍
 */
public class CopyModeDuckDbOperator implements DuckDbOperator {

    private static final Object DB_LOCK = new Object();
    
    private final Connection connection;
    private final boolean batchWritingEnabled;
    private final int batchWritingSize;
    private final long batchWritingTimeoutMs;
    private final Queue<EcommerceOrderBatch> batchBuffer;
    private final AtomicLong lastFlushTime;
    private final ScheduledExecutorService flushScheduler;
    private final Queue<Map<String, Object>> accumulatedResults;
    private volatile boolean closed = false;

    // 预编译的查询语句
    private PreparedStatement wideQuery;

    public CopyModeDuckDbOperator() throws SQLException {
        connection = DriverManager.getConnection(PerfConfig.DUCKDB_JDBC_URL);
        connection.setAutoCommit(false);
        initializeSchema();
        
        // 预编译宽表查询语句
        wideQuery = prepareWideQuery();
        
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
                Thread thread = new Thread(r, "CopyDuckDbFlushScheduler");
                thread.setDaemon(true);
                return thread;
            });
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
        statement.execute("CREATE INDEX IF NOT EXISTS idx_order_item_order_id ON order_item(order_id)");
        statement.execute("CREATE INDEX IF NOT EXISTS idx_order_main_buyer_id ON order_main(buyer_id)");
        statement.execute("CREATE INDEX IF NOT EXISTS idx_order_main_order_id ON order_main(order_id)");
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
                + "WHERE om.order_id = ?");
    }

    /**
     * 处理数据批次（使用COPY命令批量写入）
     */
    public List<Map<String, Object>> processBatch(EcommerceOrderBatch batch) throws SQLException {
        if (closed) {
            return Collections.emptyList();
        }
        
        if (batchWritingEnabled) {
            synchronized (batchBuffer) {
                batchBuffer.add(batch);
                
                if (batchBuffer.size() >= batchWritingSize) {
                    return flushInternalLocked();
                }
            }
            lastFlushTime.set(System.currentTimeMillis());
            return Collections.emptyList();
        } else {
            return processBatchInternal(Collections.singletonList(batch));
        }
    }

    private void flushIfTimeout() {
        long now = System.currentTimeMillis();
        if (now - lastFlushTime.get() >= batchWritingTimeoutMs) {
            synchronized (batchBuffer) {
                if (!batchBuffer.isEmpty()) {
                    try {
                        flushInternalLocked();
                    } catch (SQLException e) {
                        // log error but continue
                    }
                }
            }
        }
    }

    @Override
    public List<Map<String, Object>> flush() throws SQLException {
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return Collections.emptyList();
            }
            return flushInternalLocked();
        }
    }

    private List<Map<String, Object>> flushInternalLocked() throws SQLException {
        if (batchBuffer.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<EcommerceOrderBatch> batches = new ArrayList<>(batchBuffer);
        batchBuffer.clear();
        
        return processBatchInternal(batches);
    }

    /**
     * 使用COPY命令批量写入数据
     */
    private List<Map<String, Object>> processBatchInternal(List<EcommerceOrderBatch> batches) throws SQLException {
        long startNanos = System.nanoTime();
        List<Map<String, Object>> allResults = new ArrayList<>();
        int joinCount = 0;
        
        int maxAttempts = PerfConfig.MAX_TRANSACTION_RETRY;
        int attempt = 0;
        
        while (true) {
            attempt++;
            try {
                // 使用COPY命令批量写入
                writeBatchesWithCopy(batches);
                
                // 提交事务
                connection.commit();
                
                // 执行宽表查询（批量查询）
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
                
                break;
            } catch (SQLException e) {
                try {
                    connection.rollback();
                } catch (SQLException ignored) {
                }
                
                String message = e.getMessage() == null ? "" : e.getMessage();
                if ((message.contains("Catalog write-write conflict")
                        || message.contains("Conflict on update")
                        || message.contains("Current transaction is aborted")
                        || message.contains("database is locked"))
                        && attempt < maxAttempts) {
                    try {
                        Thread.sleep(PerfConfig.RETRY_DELAY_BASE_MS * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }
                throw e;
            }
        }
        
        // 记录统计
        long durationNanos = System.nanoTime() - startNanos;
        int totalRows = batches.size() * 4; // 估算：1 buyer + 1 order + 2-5 items
        StatsCollector.getInstance().recordBatch(batches.size(), totalRows, durationNanos);
        StatsCollector.getInstance().recordJoin(joinCount, System.nanoTime() - startNanos);

        // 打印DuckDB性能统计
        StatsCollector.getInstance().printStats();

        return allResults;
    }

    /**
     * 使用COPY FROM命令批量写入数据
     */
    private void writeBatchesWithCopy(List<EcommerceOrderBatch> batches) throws SQLException {
        // 使用 HashMap 去重
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
        
        // 写入buyer_info表 - 先删除已存在的记录
        StringBuilder buyerCsv = new StringBuilder();
        long buyerStart = System.nanoTime();
        for (EcommerceBuyer buyer : buyerMap.values()) {
            buyerCsv.append(toCsvRow(
                    buyer.buyerId(),
                    escapeCsv(buyer.buyerNickname()),
                    escapeCsv(buyer.buyerRealName()),
                    escapeCsv(buyer.buyerPhone()),
                    escapeCsv(buyer.buyerLevel()),
                    escapeCsv(buyer.registerArea()),
                    buyer.registerTime()));
        }
        if (buyerCsv.length() > 0) {
            // 先删除已存在的记录
            try (Statement stmt = connection.createStatement()) {
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
            executeCopyFromCsv("buyer_info", buyerCsv.toString());
        }
        StatsCollector.getInstance().recordTableWrite("buyer_info", buyerMap.size(), System.nanoTime() - buyerStart);
        
        // 写入order_main表 - 先删除已存在的记录
        StringBuilder orderCsv = new StringBuilder();
        long orderStart = System.nanoTime();
        for (EcommerceOrder order : orderMap.values()) {
            orderCsv.append(toCsvRow(
                    order.orderId(),
                    escapeCsv(order.orderNo()),
                    order.buyerId(),
                    order.createTime(),
                    order.payTime(),
                    escapeCsv(order.orderStatus()),
                    escapeCsv(order.payWay()),
                    escapeCsv(order.orderChannel()),
                    order.totalAmount(),
                    order.payAmount(),
                    order.freightAmount(),
                    order.couponAmount(),
                    escapeCsv(order.receiverName()),
                    escapeCsv(order.receiverPhone()),
                    escapeCsv(order.receiverAddress())));
        }
        if (orderCsv.length() > 0) {
            // 先删除已存在的记录
            try (Statement stmt = connection.createStatement()) {
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
            executeCopyFromCsv("order_main", orderCsv.toString());
        }
        StatsCollector.getInstance().recordTableWrite("order_main", orderMap.size(), System.nanoTime() - orderStart);
        
        // 写入order_item表 - 先删除已存在的记录
        StringBuilder itemCsv = new StringBuilder();
        long itemStart = System.nanoTime();
        int itemCount = 0;
        for (EcommerceOrderItem item : itemMap.values()) {
            itemCsv.append(toCsvRow(
                    item.itemId(),
                    item.orderId(),
                    escapeCsv(item.spuNo()),
                    escapeCsv(item.skuNo()),
                    escapeCsv(item.goodsName()),
                    escapeCsv(item.category1()),
                    escapeCsv(item.category2()),
                    escapeCsv(item.brandName()),
                    item.originalPrice(),
                    item.salePrice(),
                    item.buyNum(),
                    item.itemSubtotal(),
                    escapeCsv(item.goodsSpec())));
            itemCount++;
        }
        if (itemCsv.length() > 0) {
            // 先删除已存在的记录
            try (Statement stmt = connection.createStatement()) {
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
            executeCopyFromCsv("order_item", itemCsv.toString());
        }
        StatsCollector.getInstance().recordTableWrite("order_item", itemCount, System.nanoTime() - itemStart);
    }

    /**
     * 执行COPY FROM CSV命令
     */
    private void executeCopyFromCsv(String tableName, String csvData) throws SQLException {
        if (csvData == null || csvData.isBlank()) {
            return;
        }

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("duckdb-copy-", ".csv");
            Files.writeString(tempFile, csvData, StandardCharsets.UTF_8);

            String escapedPath = tempFile.toAbsolutePath().toString().replace("'", "''");
            String sql = String.format(
                    "COPY %s FROM '%s' (FORMAT CSV, NULL 'NULL', QUOTE '\"', ESCAPE '\"')",
                    tableName,
                    escapedPath);

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
        } catch (Exception e) {
            throw new SQLException("COPY FROM failed for table " + tableName, e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (Exception ignored) {
                    // ignore cleanup failures
                }
            }
        }
    }


    /**
     * 设置PreparedStatement参数值
     */
    private void setPreparedStatementValue(PreparedStatement pstmt, int index, String value) throws SQLException {
        if (value == null || value.equals("NULL")) {
            pstmt.setNull(index, Types.VARCHAR);
        } else {
            try {
                // 尝试解析为数字
                if (value.contains(".")) {
                    pstmt.setDouble(index, Double.parseDouble(value));
                } else {
                    pstmt.setLong(index, Long.parseLong(value));
                }
            } catch (NumberFormatException e) {
                // 不是数字，作为字符串处理
                pstmt.setString(index, value);
            }
        }
    }

    /**
     * 将数据转换为CSV行
     */
    private String toCsvRow(Object... values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            Object value = values[i];
            if (value == null) {
                sb.append("NULL");
            } else if (value instanceof Number) {
                sb.append(value);
            } else {
                sb.append("\"").append(value.toString().replace("\"", "\"\"")).append("\"");
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    /**
     * 转义CSV字符串
     */
    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\"", "\"\"");
    }

    /**
     * 解析CSV行
     */
    private String[] parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // 转义的引号
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                values.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        values.add(current.toString());
        
        return values.toArray(new String[0]);
    }

    /**
     * 批量查询宽表数据
     */
    private List<Map<String, Object>> queryWideRowsBatch(List<Long> orderIds) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        
        if (orderIds.isEmpty()) {
            return rows;
        }
        
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
        
        for (int i = 0; i < orderIds.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append("?");
        }
        sqlBuilder.append(")\n");
        
        try (PreparedStatement batchQuery = connection.prepareStatement(sqlBuilder.toString())) {
            for (int i = 0; i < orderIds.size(); i++) {
                batchQuery.setLong(i + 1, orderIds.get(i));
            }
            
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

    

    /**
     * 获取累积的查询结果
     */
    public List<Map<String, Object>> getAccumulatedResults() {
        synchronized (accumulatedResults) {
            List<Map<String, Object>> results = new ArrayList<>(accumulatedResults);
            accumulatedResults.clear();
            return results;
        }
    }

    public void close() throws Exception {
        synchronized (batchBuffer) {
            closed = true;
            
            if (flushScheduler != null) {
                flushScheduler.shutdown();
                try {
                    if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        flushScheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    flushScheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
            if (!batchBuffer.isEmpty()) {
                try {
                    flushInternalLocked();
                } catch (SQLException e) {
                    // ignore flush errors during close
                }
            }
            
            if (wideQuery != null) {
                wideQuery.close();
            }
            
            if (connection != null) {
                connection.close();
            }
        }
    }
}

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

public final class PerfConfig {

    private PerfConfig() {
    }

    // ==================== DuckDB 连接配置 ====================
    
    /**
     * DuckDB JDBC URL
     * 可通过系统属性 duckdb.jdbc.url 覆盖
     * 默认配置：内存模式，4GB内存限制，8线程，关闭WAL自动检查点，启用对象缓存
     */
    public static final String DUCKDB_JDBC_URL;

    /**
     * 连接池大小（用于共享连接模式）
     */
    public static final int CONNECTION_POOL_SIZE;

    /**
     * 连接超时时间（毫秒）
     */
    public static final int CONNECTION_TIMEOUT_MS;

    // ==================== 数据生成配置 ====================

    /**
     * 默认生成的批次数
     * 可通过系统属性 duckdb.batch.count 覆盖
     */
    public static final int DEFAULT_BATCH_COUNT;

    /**
     * 每个订单的最小订单项数
     */
    public static final int ITEMS_PER_ORDER_MIN;

    /**
     * 每个订单的最大订单项数
     */
    public static final int ITEMS_PER_ORDER_MAX;

    // ==================== 流处理配置 ====================

    /**
     * 流数据发射间隔（毫秒）
     * 可通过系统属性 duckdb.stream.emit-interval-ms 覆盖
     */
    public static final long STREAM_EMIT_INTERVAL_MS;

    /**
     * 默认流重复回放次数（0表示无限）
     * 可通过系统属性 duckdb.stream.repeat-count 覆盖
     */
    public static final int DEFAULT_STREAM_REPEAT_COUNT;

    /**
     * 数据生成QPS（每秒生成的批次数）
     * 可通过系统属性 duckdb.generation.qps 覆盖，0表示无限制
     */
    public static final int DATA_GENERATION_QPS;

    // ==================== 并发与线程配置 ====================

    /**
     * Jet Pipeline 并行度
     * 可通过系统属性 duckdb.jet.parallelism 覆盖
     */
    public static final int JET_PARALLELISM;

    /**
     * 是否启用共享服务模式（多个processor共享同一个DuckDB连接）
     */
    public static final boolean SHARED_SERVICE_MODE;

    // ==================== 攒批写入配置 ====================

    /**
     * 是否启用攒批写入模式
     * 攒批模式：累积多个批次后一次性写入 DuckDB，减少事务提交次数
     */
    public static final boolean BATCH_WRITING_ENABLED;

    /**
     * 攒批大小（批次数量）
     * 当累积的批次数达到此值时，触发一次批量写入
     */
    public static final int BATCH_WRITING_SIZE;

    /**
     * 攒批超时时间（毫秒）
     * 如果攒批未达到指定大小，但超过此时间，也会触发写入
     */
    public static final long BATCH_WRITING_TIMEOUT_MS;

    /**
     * 写入模式："insert"、"copy" 或 "arrow"
     * insert模式：使用PreparedStatement批量插入（兼容模式）
     * copy模式：使用COPY命令批量写入（高性能）
     * arrow模式：使用Arrow VectorSchemaRoot零拷贝导入（极致性能）
     */
    public static final String WRITE_MODE;

    // ==================== 事务与重试配置 ====================

    /**
     * 事务重试最大次数
     */
    public static final int MAX_TRANSACTION_RETRY;

    /**
     * 重试间隔基数（毫秒），实际间隔 = RETRY_DELAY_BASE_MS * attempt
     */
    public static final long RETRY_DELAY_BASE_MS;

    // ==================== 统计配置 ====================

    /**
     * 是否启用实时滚动统计
     * 可通过系统属性 duckdb.stats.rolling.enabled 覆盖
     */
    public static final boolean ROLLING_STATS_ENABLED;

    /**
     * 滚动统计窗口大小（秒）
     */
    public static final int ROLLING_STATS_WINDOW_SECONDS;

    /**
     * 是否输出按表统计详情
     */
    public static final boolean TABLE_STATS_DETAILED;

    /**
     * 是否打印数据源(source)性能指标
     * 可通过系统属性 duckdb.stats.source.enabled 覆盖
     */
    public static final boolean SOURCE_STATS_ENABLED;

    // ==================== Hazelcast 数据结构名称 ====================

    /**
     * 源数据列表名称
     */
    public static final String SOURCE_LIST_NAME;

    /**
     * 结果输出列表名称
     */
    public static final String RESULT_LIST_NAME;

    // ==================== 初始化静态块 ====================

    static {
        // DuckDB JDBC URL
        String jdbcUrl = System.getProperty("duckdb.jdbc.url");
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            // 优化后的默认配置：
            // - 8GB内存限制
            // - 线程数等于CPU核心数
            // - 禁用WAL（大幅提升写入性能）
            // - 禁用检查点
            // - 启用对象缓存
            int cpuCores = Runtime.getRuntime().availableProcessors();
            jdbcUrl = String.format("jdbc:duckdb:memory:?memory_limit=8GB&threads=%d&wal_enabled=false&checkpoint_threshold=0&enable_object_cache=true", cpuCores);
        }
        DUCKDB_JDBC_URL = jdbcUrl;

        // 连接池配置
        CONNECTION_POOL_SIZE = Integer.getInteger("duckdb.connection.pool-size", 4);
        CONNECTION_TIMEOUT_MS = Integer.getInteger("duckdb.connection.timeout-ms", 30000);

        // 数据生成配置
        DEFAULT_BATCH_COUNT = Integer.getInteger("duckdb.batch.count", 100);
        ITEMS_PER_ORDER_MIN = Integer.getInteger("duckdb.items.per-order-min", 2);
        ITEMS_PER_ORDER_MAX = Integer.getInteger("duckdb.items.per-order-max", 5);

        // 流处理配置
        STREAM_EMIT_INTERVAL_MS = Long.getLong("duckdb.stream.emit-interval-ms", 0L);
        DEFAULT_STREAM_REPEAT_COUNT = Integer.getInteger("duckdb.stream.repeat-count", 1);
        // 数据生成QPS（0表示无限制）
        DATA_GENERATION_QPS = Integer.getInteger("duckdb.generation.qps", 1000);

        // 并发配置
        JET_PARALLELISM = Integer.getInteger("duckdb.jet.parallelism", Runtime.getRuntime().availableProcessors());
        // 默认启用共享服务模式，让所有processor共用一个operator实例，避免DuckDB内存数据库多连接问题
        SHARED_SERVICE_MODE = Boolean.parseBoolean(System.getProperty("duckdb.service.shared", "true"));

        // 攒批写入配置
        BATCH_WRITING_ENABLED = Boolean.parseBoolean(System.getProperty("duckdb.batch-writing.enabled", "true"));
        BATCH_WRITING_SIZE = Integer.getInteger("duckdb.batch-writing.size", 100);
        BATCH_WRITING_TIMEOUT_MS = Long.getLong("duckdb.batch-writing.timeout-ms", 1000L);
        // 写入模式：insert、copy 或 arrow
        String writeMode = System.getProperty("duckdb.write.mode", "insert");
        if (!"copy".equalsIgnoreCase(writeMode) && !"insert".equalsIgnoreCase(writeMode) && !"arrow".equalsIgnoreCase(writeMode)) {
            writeMode = "insert";
        }
        WRITE_MODE = writeMode.toLowerCase();

        // 事务重试配置
        MAX_TRANSACTION_RETRY = Integer.getInteger("duckdb.transaction.max-retry", 5);
        RETRY_DELAY_BASE_MS = Long.getLong("duckdb.transaction.retry-delay-ms", 20L);

        // 统计配置
        ROLLING_STATS_ENABLED = Boolean.parseBoolean(System.getProperty("duckdb.stats.rolling.enabled", "true"));
        ROLLING_STATS_WINDOW_SECONDS = Integer.getInteger("duckdb.stats.rolling.window-seconds", 5);
        TABLE_STATS_DETAILED = Boolean.parseBoolean(System.getProperty("duckdb.stats.table-detailed", "true"));
        SOURCE_STATS_ENABLED = Boolean.parseBoolean(System.getProperty("duckdb.stats.source.enabled", "true"));

        // 列表名称
        SOURCE_LIST_NAME = System.getProperty("duckdb.list.source-name", "ecommerce-source");
        RESULT_LIST_NAME = System.getProperty("duckdb.list.result-name", "ecommerce-output");
    }

    /**
     * 获取当前配置摘要，用于日志输出
     */
    public static String getConfigSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== DuckDB 性能配置摘要 ===\n");
        sb.append("JDBC URL: ").append(DUCKDB_JDBC_URL).append("\n");
        sb.append("连接池大小: ").append(CONNECTION_POOL_SIZE).append("\n");
        sb.append("批次数: ").append(DEFAULT_BATCH_COUNT).append("\n");
        sb.append("每订单项数范围: ").append(ITEMS_PER_ORDER_MIN).append("-").append(ITEMS_PER_ORDER_MAX).append("\n");
        sb.append("流发射间隔: ").append(STREAM_EMIT_INTERVAL_MS).append("ms\n");
        sb.append("Jet并行度: ").append(JET_PARALLELISM).append("\n");
        sb.append("共享服务模式: ").append(SHARED_SERVICE_MODE).append("\n");
        sb.append("写入模式: ").append(WRITE_MODE.toUpperCase()).append("\n");
        sb.append("攒批写入: ").append(BATCH_WRITING_ENABLED).append(" (大小: ").append(BATCH_WRITING_SIZE)
                .append(", 超时: ").append(BATCH_WRITING_TIMEOUT_MS).append("ms)\n");
        sb.append("最大重试次数: ").append(MAX_TRANSACTION_RETRY).append("\n");
        sb.append("滚动统计: ").append(ROLLING_STATS_ENABLED).append("\n");
        sb.append("============================\n");
        return sb.toString();
    }
}
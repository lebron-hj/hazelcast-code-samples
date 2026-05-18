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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能统计收集器 - 支持多时间窗口的全方位统计
 * 包括：总体QPS、各表TPS、最新JOIN统计、窗口内平均延迟
 */
public final class StatsCollector {

    private static final StatsCollector INSTANCE = new StatsCollector();

    // ANSI 颜色常量
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";

    // ========== 时间窗口配置 ==========
    private static final int WINDOW_30_SEC = 30;
    private static final int WINDOW_1_MIN = 60;
    private static final int WINDOW_5_MIN = 300;
    private static final int WINDOW_10_MIN = 600;
    
    // 最大采样数 = 10分钟 = 600个1秒采样点
    private static final int MAX_SAMPLES = WINDOW_10_MIN;
    
    // ========== 累计统计 ==========
    private final AtomicLong totalBatches = new AtomicLong();
    private final AtomicLong totalRows = new AtomicLong();
    private final AtomicLong totalNanos = new AtomicLong();
    private final AtomicLong totalRetries = new AtomicLong();
    private final Map<String, AtomicLong> tableRows = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> tableNanos = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> tableRetries = new ConcurrentHashMap<>();
    private final AtomicLong joinRows = new AtomicLong();
    private final AtomicLong joinNanos = new AtomicLong();
    
    // ========== 延迟统计（用于计算百分位） ==========
    private final AtomicLong minBatchNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxBatchNanos = new AtomicLong(0);
    private final AtomicLong sumBatchNanos = new AtomicLong(0);
    private final AtomicLong countBatchNanos = new AtomicLong(0);
    
    // 滑动窗口延迟采样（用于计算百分位）
    private static final int LATENCY_WINDOW_SIZE = 1024;
    private final long[] latencySamples = new long[LATENCY_WINDOW_SIZE];
    private volatile int latencyIndex = 0;
    
    // ========== 多时间窗口滚动统计 ==========
    
    // 总体统计
    private final long[] sampleTimestamps = new long[MAX_SAMPLES];
    private final long[] deltaTotalRows = new long[MAX_SAMPLES];
    private final long[] deltaTotalNanos = new long[MAX_SAMPLES];
    private final long[] deltaBatchCount = new long[MAX_SAMPLES];
    
    // 各表写入统计
    private final Map<String, long[]> deltaTableRows = new ConcurrentHashMap<>();
    
    // JOIN 最新统计
    private volatile long latestJoinRows = 0;
    private volatile long latestJoinNanos = 0;
    private volatile boolean hasLatestJoin = false;
    
    // 循环缓冲区索引
    private volatile int sampleIndex = 0;
    private volatile int totalSamples = 0;
    
    private final ScheduledExecutorService scheduler;
    private final boolean rollingEnabled;
    private final boolean tableStatsDetailed;
    
    // 上次采样值（用于计算增量）
    private volatile long lastTotalRows = 0;
    private volatile long lastSumBatchNanos = 0;
    private volatile long lastCountBatchNanos = 0;
    private final Map<String, AtomicLong> lastTableRows = new ConcurrentHashMap<>();

    // 启动时间戳
    private volatile long startNanos = System.nanoTime();

    private StatsCollector() {
        // 初始化表统计（直接硬编码）
        tableRows.put("buyer_info", new AtomicLong());
        tableRows.put("order_main", new AtomicLong());
        tableRows.put("order_item", new AtomicLong());
        
        tableNanos.put("buyer_info", new AtomicLong());
        tableNanos.put("order_main", new AtomicLong());
        tableNanos.put("order_item", new AtomicLong());
        
        tableRetries.put("buyer_info", new AtomicLong());
        tableRetries.put("order_main", new AtomicLong());
        tableRetries.put("order_item", new AtomicLong());
        
        // 初始化上次采样值
        lastTableRows.put("buyer_info", new AtomicLong());
        lastTableRows.put("order_main", new AtomicLong());
        lastTableRows.put("order_item", new AtomicLong());
        
        // 初始化滚动统计数组
        deltaTableRows.put("buyer_info", new long[MAX_SAMPLES]);
        deltaTableRows.put("order_main", new long[MAX_SAMPLES]);
        deltaTableRows.put("order_item", new long[MAX_SAMPLES]);
        
        // 读取配置
        rollingEnabled = PerfConfig.ROLLING_STATS_ENABLED;
        tableStatsDetailed = PerfConfig.TABLE_STATS_DETAILED;
        
        // 启动滚动统计线程 - 每1秒采样一次
        if (rollingEnabled) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "StatsCollector-rolling");
                thread.setDaemon(true);
                return thread;
            });
            scheduler.scheduleAtFixedRate(this::sampleAndMaybePrint, 1, 1, TimeUnit.SECONDS);
        } else {
            scheduler = null;
        }
    }

    public static StatsCollector getInstance() {
        return INSTANCE;
    }

    /**
     * 记录批次处理统计
     */
    public void recordBatch(long batches, long rows, long nanos) {
        totalBatches.addAndGet(batches);
        totalRows.addAndGet(rows);
        totalNanos.addAndGet(nanos);
        countBatchNanos.addAndGet(batches);
        
        // 计算单批次平均延迟
        long nanosPerBatch = batches > 0 ? nanos / batches : 0;
        
        // 更新延迟统计 - 使用单批次延迟
        updateMinMax(nanosPerBatch);
        sumBatchNanos.addAndGet(nanos);
        
        // 记录延迟样本用于百分位计算 - 使用单批次延迟
        recordLatencySample(nanosPerBatch);
    }

    /**
     * 记录重试次数
     */
    public void recordRetry() {
        totalRetries.incrementAndGet();
    }

    /**
     * 记录表写入统计
     */
    public void recordTableWrite(String table, long rows, long nanos) {
        tableRows.computeIfAbsent(table, ignored -> new AtomicLong()).addAndGet(rows);
        tableNanos.computeIfAbsent(table, ignored -> new AtomicLong()).addAndGet(nanos);
    }

    /**
     * 记录表写入重试
     */
    public void recordTableRetry(String table) {
        tableRetries.computeIfAbsent(table, ignored -> new AtomicLong()).incrementAndGet();
    }

    /**
     * 记录 JOIN 查询统计（同时更新最新JOIN统计）
     */
    public void recordJoin(long rows, long nanos) {
        joinRows.addAndGet(rows);
        joinNanos.addAndGet(nanos);
        
        // 更新最新JOIN统计
        latestJoinRows = rows;
        latestJoinNanos = nanos;
        hasLatestJoin = true;
    }

    /**
     * 记录延迟样本
     */
    private void recordLatencySample(long nanos) {
        int idx = (int) (System.nanoTime() % LATENCY_WINDOW_SIZE);
        latencySamples[idx] = nanos;
        latencyIndex = idx;
    }

    /**
     * 计算延迟百分位
     */
    private double[] calculateLatencyPercentiles() {
        long[] validSamples = Arrays.stream(latencySamples)
                .filter(v -> v > 0)
                .sorted()
                .toArray();
        
        if (validSamples.length == 0) {
            return new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
        }
        
        int len = validSamples.length;
        double p50 = validSamples[(int) (len * 0.50)] / 1_000_000.0;
        double p90 = validSamples[(int) (len * 0.90)] / 1_000_000.0;
        double p95 = validSamples[(int) (len * 0.95)] / 1_000_000.0;
        double p99 = validSamples[(int) (len * 0.99)] / 1_000_000.0;
        double avg = Arrays.stream(validSamples).average().orElse(0.0) / 1_000_000.0;
        
        return new double[]{avg, p50, p90, p95, p99};
    }

    /**
     * 重置所有统计
     */
    public void reset() {
        totalBatches.set(0);
        totalRows.set(0);
        totalNanos.set(0);
        totalRetries.set(0);
        minBatchNanos.set(Long.MAX_VALUE);
        maxBatchNanos.set(0);
        sumBatchNanos.set(0);
        countBatchNanos.set(0);
        
        tableRows.values().forEach(value -> value.set(0));
        tableNanos.values().forEach(value -> value.set(0));
        tableRetries.values().forEach(value -> value.set(0));
        
        joinRows.set(0);
        joinNanos.set(0);
        
        // 重置滚动统计
        Arrays.fill(sampleTimestamps, 0L);
        Arrays.fill(deltaTotalRows, 0L);
        Arrays.fill(deltaTotalNanos, 0L);
        Arrays.fill(deltaBatchCount, 0L);
        deltaTableRows.values().forEach(arr -> Arrays.fill(arr, 0L));
        Arrays.fill(latencySamples, 0L);
        sampleIndex = 0;
        totalSamples = 0;
        
        // 重置最新JOIN统计
        latestJoinRows = 0;
        latestJoinNanos = 0;
        hasLatestJoin = false;
        
        // 重置上次采样值
        lastTotalRows = 0;
        lastSumBatchNanos = 0;
        lastCountBatchNanos = 0;
        lastTableRows.values().forEach(v -> v.set(0));
        
        // 重置启动时间
        startNanos = System.nanoTime();
    }

    /**
     * 打印汇总统计
     */
    public void printSummary() {
        long batches = totalBatches.get();
        long rows = totalRows.get();
        long nanos = totalNanos.get();
        double seconds = nanos / 1_000_000_000.0;
        double qps = seconds > 0.0 ? rows / seconds : 0.0;
        double batchesPerSecond = seconds > 0.0 ? batches / seconds : 0.0;
        double avgBatchMs = batches > 0 ? (nanos / 1_000_000.0) / batches : 0.0;
        
        // 计算延迟统计
        double minBatchMs = minBatchNanos.get() == Long.MAX_VALUE ? 0.0 : minBatchNanos.get() / 1_000_000.0;
        double maxBatchMs = maxBatchNanos.get() / 1_000_000.0;
        double[] percentiles = calculateLatencyPercentiles();
        
        System.out.println("\n" + "=".repeat(70));
        System.out.println("               DuckDB 写入性能测试汇总统计");
        System.out.println("=".repeat(70));
        
        // 总体吞吐统计
        System.out.println("\n【总体吞吐统计】");
        System.out.printf("  %-20s %,d%n", "总批次数:", batches);
        System.out.printf("  %-20s %,d%n", "总输出行数:", rows);
        System.out.printf("  %-20s %.3f 秒%n", "总耗时:", seconds);
        System.out.printf("  %-20s %s万 (行/秒)%n", "QPS:", formatQpsTps(qps));
        System.out.printf("  %-20s %.2f 批/秒%n", "批次吞吐:", batchesPerSecond);
        
        // 延迟统计
        System.out.println("\n【延迟统计 (毫秒)】");
        System.out.printf("  %-20s %.3f%n", "平均延迟:", percentiles[0]);
        System.out.printf("  %-20s %.3f%n", "最小延迟:", minBatchMs);
        System.out.printf("  %-20s %.3f%n", "最大延迟:", maxBatchMs);
        System.out.printf("  %-20s P50: %.3f | P90: %.3f | P95: %.3f | P99: %.3f%n", 
                "延迟百分位:", percentiles[1], percentiles[2], percentiles[3], percentiles[4]);
        
        // 重试统计
        System.out.println("\n【重试统计】");
        System.out.printf("  %-20s %,d%n", "总重试次数:", totalRetries.get());
        double retryRate = batches > 0 ? (double) totalRetries.get() / batches * 100 : 0.0;
        System.out.printf("  %-20s %.4f%%%n", "重试率:", retryRate);
        
        // 按表统计
        if (tableStatsDetailed) {
            System.out.println("\n【按表写入统计】");
            
            // 直接遍历三个表
            for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                long tableRowCount = tableRows.getOrDefault(table, new AtomicLong()).get();
                long tableNanosValue = tableNanos.getOrDefault(table, new AtomicLong()).get();
                long tableRetryCount = tableRetries.getOrDefault(table, new AtomicLong()).get();
                double tableSeconds = tableNanosValue / 1_000_000_000.0;
                double tableQps = tableSeconds > 0.0 ? tableRowCount / tableSeconds : 0.0;
                double avgRowMs = tableRowCount > 0 ? (tableNanosValue / 1_000_000.0) / tableRowCount : 0.0;
                double tableRetryRate = tableRowCount > 0 ? (double) tableRetryCount / tableRowCount * 100 : 0.0;
                
                System.out.printf("  ├─ %s:%n", table);
                System.out.printf("  │   %-16s %,d%n", "行数:", tableRowCount);
                System.out.printf("  │   %-16s %s万%n", "QPS:", formatQpsTps(tableQps));
                System.out.printf("  │   %-16s %.4f 毫秒%n", "平均每行延迟:", avgRowMs);
                System.out.printf("  │   %-16s %,d (%.4f%%)%n", "重试次数:", tableRetryCount, tableRetryRate);
            }
        }
        
        // JOIN 查询统计
        long joinRowCount = joinRows.get();
        long joinNanosValue = joinNanos.get();
        double joinSeconds = joinNanosValue / 1_000_000_000.0;
        double joinQps = joinSeconds > 0.0 ? joinRowCount / joinSeconds : 0.0;
        
        System.out.println("\n【宽表查询 (Join) 统计】");
        System.out.printf("  %-20s %,d%n", "返回行数:", joinRowCount);
        System.out.printf("  %-20s %s万%n", "QPS:", formatQpsTps(joinQps));
        System.out.printf("  %-20s %.3f 秒%n", "总耗时:", joinSeconds);
        
        System.out.println("\n" + "=".repeat(70));
    }

    /**
     * 打印完整统计报告（支持滚动统计和普通统计）
     */
    /**
     * 格式化 QPS/TPS 为万单位，便于查看
     * @param value QPS/TPS 值
     * @return 格式化后的字符串（例如 "2.4" 表示 2.4万）
     */
    private String formatQpsTps(double value) {
        if (value < 1000.0) {
            return String.format("%.2f", value / 10000.0);
        } else if (value < 10000.0) {
            return String.format("%.1f", value / 10000.0);
        } else {
            return String.format("%.0f", value / 10000.0);
        }
    }
    
    /**
     * 格式化 Map 中的 QPS/TPS 为万单位
     * @param map 包含表名和对应 TPS 的 Map
     * @return 包含表名和格式化后 TPS 的新 Map
     */
    private Map<String, String> formatQpsTpsMap(Map<String, Double> map) {
        Map<String, String> result = new HashMap<>();
        map.forEach((key, value) -> result.put(key, formatQpsTps(value)));
        return result;
    }
    
    public void printStats() {
        long currentTotalRows = totalRows.get();
        long endNanos = System.nanoTime();
        long totalElapsedNanos = endNanos - startNanos;
        
        double totalSeconds = totalElapsedNanos / 1_000_000_000.0;
        double avgQps = currentTotalRows > 0 ? currentTotalRows / totalSeconds : 0.0;
        
        System.out.println("\n========== 完整统计报告 ==========");
        System.out.printf("%-20s %,d\n", "总行数:", currentTotalRows);
        System.out.printf("%-20s %.2f s\n", "总耗时:", totalSeconds);
        System.out.printf("%-20s %s万\n", "平均 QPS:", formatQpsTps(avgQps));
        
        // 如果启用了滚动统计，显示多时间窗口统计
        if (rollingEnabled && totalSamples > 0) {
            System.out.println("\n【滚动时间窗口统计】");
            
            double qps30 = calculateQpsForWindow(WINDOW_30_SEC);
            double qps60 = calculateQpsForWindow(WINDOW_1_MIN);
            double qps300 = calculateQpsForWindow(WINDOW_5_MIN);
            double qps600 = calculateQpsForWindow(WINDOW_10_MIN);
            
            System.out.printf("  ├─ %-18s %s/%s/%s/%s万\n", "QPS[30s/1m/5m/10m]:", 
                    formatQpsTps(qps30), formatQpsTps(qps60), formatQpsTps(qps300), formatQpsTps(qps600));
            
            double avgLatency30 = calculateAvgLatencyForWindow(WINDOW_30_SEC);
            double avgLatency60 = calculateAvgLatencyForWindow(WINDOW_1_MIN);
            double avgLatency300 = calculateAvgLatencyForWindow(WINDOW_5_MIN);
            double avgLatency600 = calculateAvgLatencyForWindow(WINDOW_10_MIN);
            
            System.out.printf("  ├─ %-18s %.2f/%.2f/%.2f/%.2f ms\n", "平均延迟[30s/1m/5m/10m]:", avgLatency30, avgLatency60, avgLatency300, avgLatency600);
            
            if (hasLatestJoin) {
                System.out.printf("  ├─ %-18s %,d 行, %.3f ms\n", "最后 JOIN 查询:", latestJoinRows, latestJoinNanos / 1_000_000.0);
            }
            
            if (tableStatsDetailed) {
                Map<String, Double> tps30 = calculateTableTpsForWindow(WINDOW_30_SEC);
                Map<String, Double> tps60 = calculateTableTpsForWindow(WINDOW_1_MIN);
                Map<String, Double> tps300 = calculateTableTpsForWindow(WINDOW_5_MIN);
                Map<String, Double> tps600 = calculateTableTpsForWindow(WINDOW_10_MIN);
                
                System.out.println("\n【按表写入时间窗口统计】");
                for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                    double t30 = tps30.getOrDefault(table, 0.0);
                    double t60 = tps60.getOrDefault(table, 0.0);
                    double t300 = tps300.getOrDefault(table, 0.0);
                    double t600 = tps600.getOrDefault(table, 0.0);
                    System.out.printf("  ├─ %s TPS[30s/1m/5m/10m]: %s/%s/%s/%s万\n", 
                            table, formatQpsTps(t30), formatQpsTps(t60), formatQpsTps(t300), formatQpsTps(t600));
                }
            }
        } else {
            // 普通统计模式
            long batches = totalBatches.get();
            long rows = totalRows.get();
            long nanos = totalNanos.get();
            double seconds = nanos / 1_000_000_000.0;
            double qps = seconds > 0.0 ? rows / seconds : 0.0;
            double avgLatencyMs = batches > 0 ? (nanos / 1_000_000.0) / batches : 0.0;

            // JOIN统计
            long joinRowCount = joinRows.get();
            long joinNanosValue = joinNanos.get();
            double joinSeconds = joinNanosValue / 1_000_000_000.0;
            double joinQps = joinSeconds > 0.0 ? joinRowCount / joinSeconds : 0.0;

            // 各表写入统计（包含TPS）
            long buyerRows = tableRows.getOrDefault("buyer_info", new AtomicLong()).get();
            long buyerNanos = tableNanos.getOrDefault("buyer_info", new AtomicLong()).get();
            double buyerTps = buyerNanos > 0 ? buyerRows / (buyerNanos / 1_000_000_000.0) : 0.0;

            long orderRows = tableRows.getOrDefault("order_main", new AtomicLong()).get();
            long orderNanos = tableNanos.getOrDefault("order_main", new AtomicLong()).get();
            double orderTps = orderNanos > 0 ? orderRows / (orderNanos / 1_000_000_000.0) : 0.0;

            long itemRows = tableRows.getOrDefault("order_item", new AtomicLong()).get();
            long itemNanos = tableNanos.getOrDefault("order_item", new AtomicLong()).get();
            double itemTps = itemNanos > 0 ? itemRows / (itemNanos / 1_000_000_000.0) : 0.0;

            System.out.printf("%n[DUCKDB] QPS: %s万 | 总批次: %,d | 总行数: %,d | 平均延迟: %.2fms | joinSeconds: %.2fms | joinRowCount: %,d | JOIN-QPS: %s万 | 表写入: buyer=%d(%s万TPS) order=%d(%s万TPS) item=%d(%s万TPS)%n",
                    formatQpsTps(qps), batches, rows, avgLatencyMs, joinSeconds, joinRowCount, formatQpsTps(joinQps),
                    buyerRows, formatQpsTps(buyerTps), orderRows, formatQpsTps(orderTps), itemRows, formatQpsTps(itemTps));
        }
        
        // 按表详细统计（始终显示）
        if (tableStatsDetailed) {
            System.out.println("\n【按表写入详细统计】");
            
            for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                long tableRowCount = tableRows.getOrDefault(table, new AtomicLong()).get();
                long tableNanosValue = tableNanos.getOrDefault(table, new AtomicLong()).get();
                long tableRetryCount = tableRetries.getOrDefault(table, new AtomicLong()).get();
                double tableSeconds = tableNanosValue / 1_000_000_000.0;
                double tableQps = tableSeconds > 0.0 ? tableRowCount / tableSeconds : 0.0;
                double avgRowMs = tableRowCount > 0 ? (tableNanosValue / 1_000_000.0) / tableRowCount : 0.0;
                double tableRetryRate = tableRowCount > 0 ? (double) tableRetryCount / tableRowCount * 100 : 0.0;
                
                System.out.printf("  ├─ %s:%n", table);
                System.out.printf("  │   %-16s %,d\n", "行数:", tableRowCount);
                System.out.printf("  │   %-16s %s万\n", "QPS:", formatQpsTps(tableQps));
                System.out.printf("  │   %-16s %.4f 毫秒\n", "平均每行延迟:", avgRowMs);
                System.out.printf("  │   %-16s %,d (%.4f%%)\n", "重试次数:", tableRetryCount, tableRetryRate);
            }
        }
        
        System.out.println("====================================");
    }

    /**
     * 采样并打印滚动统计 - 支持多时间窗口的全方位统计
     */
    private void sampleAndMaybePrint() {
        try {
            long now = System.currentTimeMillis();
            long currentTotalRows = totalRows.get();
            long currentSumBatchNanos = sumBatchNanos.get();
            long currentCountBatchNanos = countBatchNanos.get();
            
            // ========== 第一步：计算增量 ==========
            long deltaRows = currentTotalRows - lastTotalRows;
            long deltaSumNanos = currentSumBatchNanos - lastSumBatchNanos;
            long deltaCountBatches = currentCountBatchNanos - lastCountBatchNanos;
            
            // ========== 第二步：计算各表增量 ==========
            Map<String, Long> tableDeltaMap = new HashMap<>();
            if (tableStatsDetailed) {
                for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                    long currentTableRowValue = tableRows.getOrDefault(table, new AtomicLong()).get();
                    long lastTableRowValue = lastTableRows.getOrDefault(table, new AtomicLong()).get();
                    long tableDelta = currentTableRowValue - lastTableRowValue;
                    tableDeltaMap.put(table, tableDelta);
                }
            }
            
            // ========== 第三步：存储增量到循环缓冲区 ==========
            sampleTimestamps[sampleIndex] = now;
            deltaTotalRows[sampleIndex] = deltaRows;
            deltaTotalNanos[sampleIndex] = deltaSumNanos;
            deltaBatchCount[sampleIndex] = deltaCountBatches;
            
            // 存储各表增量
            if (tableStatsDetailed) {
                tableDeltaMap.forEach((table, delta) -> {
                    long[] tableDeltaArray = deltaTableRows.get(table);
                    if (tableDeltaArray != null) {
                        tableDeltaArray[sampleIndex] = delta;
                    }
                });
            }
            
            // 更新循环缓冲区索引
            int currentIndex = sampleIndex;
            sampleIndex = (sampleIndex + 1) % MAX_SAMPLES;
            if (totalSamples < MAX_SAMPLES) {
                totalSamples++;
            }
            
            // ========== 第四步：计算各时间窗口的统计 ==========
            if (totalSamples > 0) {
                // 计算总体 QPS
                double qps30Sec = calculateQpsForWindow(WINDOW_30_SEC);
                double qps1Min = calculateQpsForWindow(WINDOW_1_MIN);
                double qps5Min = calculateQpsForWindow(WINDOW_5_MIN);
                double qps10Min = calculateQpsForWindow(WINDOW_10_MIN);
                
                // 计算窗口内平均延迟
                double avgLatency30Sec = calculateAvgLatencyForWindow(WINDOW_30_SEC);
                double avgLatency1Min = calculateAvgLatencyForWindow(WINDOW_1_MIN);
                double avgLatency5Min = calculateAvgLatencyForWindow(WINDOW_5_MIN);
                double avgLatency10Min = calculateAvgLatencyForWindow(WINDOW_10_MIN);
                
                // 计算各表 TPS
                Map<String, Double> tableTps30Sec = calculateTableTpsForWindow(WINDOW_30_SEC);
                Map<String, Double> tableTps1Min = calculateTableTpsForWindow(WINDOW_1_MIN);
                Map<String, Double> tableTps5Min = calculateTableTpsForWindow(WINDOW_5_MIN);
                Map<String, Double> tableTps10Min = calculateTableTpsForWindow(WINDOW_10_MIN);
                
                // ========== 第五步：打印统计信息 ==========
                StringBuilder sb = new StringBuilder();
                sb.append("\r");
                sb.append("[实时] ");
                
                // 最近1秒的行数
                sb.append(String.format("%-18s", "实时QPS: " + formatQpsTps(deltaRows) + "万"));
                
                // 多时间窗口总体 QPS（万单位）
                sb.append(String.format("| QPS[30s/1m/5m/10m]: %s/%s/%s/%s万 ",
                        formatQpsTps(qps30Sec), formatQpsTps(qps1Min), formatQpsTps(qps5Min), formatQpsTps(qps10Min)));
                
                // 各表 TPS（显示4个时间窗口，万单位）
                if (tableStatsDetailed) {
                    sb.append("| ");
                    for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                        double tps30 = tableTps30Sec.getOrDefault(table, 0.0);
                        double tps60 = tableTps1Min.getOrDefault(table, 0.0);
                        double tps300 = tableTps5Min.getOrDefault(table, 0.0);
                        double tps600 = tableTps10Min.getOrDefault(table, 0.0);
                        sb.append(table).append("[30s/1m/5m/10m]:").append(
                                String.format("%s/%s/%s/%s万",
                                        formatQpsTps(tps30), formatQpsTps(tps60), formatQpsTps(tps300), formatQpsTps(tps600))
                        ).append(" ");
                    }
                }
                
                // 最新JOIN统计
                if (hasLatestJoin) {
                    sb.append(String.format("| JOIN[last]: %,d行/%.3fms ", 
                            latestJoinRows, latestJoinNanos / 1_000_000.0));
                } else {
                    sb.append("| JOIN[last]: 暂无 ");
                }
                
                // 窗口内平均延迟（显示4个时间窗口）
                sb.append(String.format("| 平均延迟[30s/1m/5m/10m]: %.2f/%.2f/%.2f/%.2fms", 
                        avgLatency30Sec, avgLatency1Min, avgLatency5Min, avgLatency10Min));
                
                System.out.print(sb);
                System.out.flush();
            }
            
            // ========== 第六步：更新上次采样值 ==========
            lastTotalRows = currentTotalRows;
            lastSumBatchNanos = currentSumBatchNanos;
            lastCountBatchNanos = currentCountBatchNanos;
            
            if (tableStatsDetailed) {
                tableDeltaMap.forEach((table, delta) -> {
                    AtomicLong lastVal = lastTableRows.get(table);
                    if (lastVal != null) {
                        long currentVal = lastVal.get() + delta;
                        lastVal.set(currentVal);
                    }
                });
            }
            
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
    
    /**
     * 计算指定时间窗口的总体 QPS（使用实际时间戳提高精度）
     */
    private double calculateQpsForWindow(int windowSize) {
        if (totalSamples == 0) {
            return 0.0;
        }
        
        int actualSamples = Math.min(windowSize, totalSamples);
        long totalRowsInWindow = 0;
        long firstTimestamp = 0;
        long lastTimestamp = 0;
        
        // 从循环缓冲区中读取最近 N 个样本
        for (int i = 0; i < actualSamples; i++) {
            int idx = (sampleIndex - 1 - i + MAX_SAMPLES) % MAX_SAMPLES;
            if (sampleTimestamps[idx] > 0) {
                totalRowsInWindow += deltaTotalRows[idx];
                if (i == 0) {
                    lastTimestamp = sampleTimestamps[idx];
                }
                if (i == actualSamples - 1) {
                    firstTimestamp = sampleTimestamps[idx];
                }
            }
        }
        
        // 使用实际时间差计算窗口时间，提高精度
        double windowSeconds;
        if (lastTimestamp > firstTimestamp && firstTimestamp > 0) {
            windowSeconds = (lastTimestamp - firstTimestamp) / 1000.0;
            // 防止时间差太小，使用实际采样数量作为下限
            if (windowSeconds < 0.1) {
                windowSeconds = actualSamples;
            }
        } else {
            windowSeconds = actualSamples;
        }
        
        return windowSeconds > 0 ? totalRowsInWindow / windowSeconds : 0.0;
    }
    
    /**
     * 计算指定时间窗口的各表 TPS（使用实际时间戳提高精度）
     */
    private Map<String, Double> calculateTableTpsForWindow(int windowSize) {
        Map<String, Double> result = new HashMap<>();
        
        if (totalSamples == 0) {
            return result;
        }
        
        int actualSamples = Math.min(windowSize, totalSamples);
        
        // 首先计算窗口时间
        double windowSeconds = calculateWindowSeconds(actualSamples);
        
        for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
            long[] tableDeltaArray = deltaTableRows.get(table);
            if (tableDeltaArray == null) {
                continue;
            }
            
            long totalRowsInWindow = 0;
            for (int i = 0; i < actualSamples; i++) {
                int idx = (sampleIndex - 1 - i + MAX_SAMPLES) % MAX_SAMPLES;
                if (sampleTimestamps[idx] > 0) {
                    totalRowsInWindow += tableDeltaArray[idx];
                }
            }
            
            double tps = windowSeconds > 0 ? totalRowsInWindow / windowSeconds : 0.0;
            result.put(table, tps);
        }
        
        return result;
    }
    
    /**
     * 计算指定采样数量的窗口时间（使用实际时间戳）
     */
    private double calculateWindowSeconds(int actualSamples) {
        if (actualSamples <= 0 || totalSamples == 0) {
            return actualSamples;
        }
        
        long firstTimestamp = 0;
        long lastTimestamp = 0;
        
        for (int i = 0; i < actualSamples; i++) {
            int idx = (sampleIndex - 1 - i + MAX_SAMPLES) % MAX_SAMPLES;
            if (sampleTimestamps[idx] > 0) {
                if (i == 0) {
                    lastTimestamp = sampleTimestamps[idx];
                }
                if (i == actualSamples - 1) {
                    firstTimestamp = sampleTimestamps[idx];
                }
            }
        }
        
        double windowSeconds;
        if (lastTimestamp > firstTimestamp && firstTimestamp > 0) {
            windowSeconds = (lastTimestamp - firstTimestamp) / 1000.0;
            if (windowSeconds < 0.1) {
                windowSeconds = actualSamples;
            }
        } else {
            windowSeconds = actualSamples;
        }
        
        return windowSeconds;
    }
    
    /**
     * 计算指定时间窗口的平均延迟
     */
    private double calculateAvgLatencyForWindow(int windowSize) {
        if (totalSamples == 0) {
            return 0.0;
        }
        
        int actualSamples = Math.min(windowSize, totalSamples);
        long totalNanosInWindow = 0;
        long totalBatchesInWindow = 0;
        
        // 从循环缓冲区中读取最近 N 个样本
        for (int i = 0; i < actualSamples; i++) {
            int idx = (sampleIndex - 1 - i + MAX_SAMPLES) % MAX_SAMPLES;
            if (sampleTimestamps[idx] > 0) {
                totalNanosInWindow += deltaTotalNanos[idx];
                totalBatchesInWindow += deltaBatchCount[idx];
            }
        }
        
        return totalBatchesInWindow > 0 ? (totalNanosInWindow / (double) totalBatchesInWindow) / 1_000_000.0 : 0.0;
    }

    /**
     * 获取当前QPS值（用于外部监控）
     */
    public double getCurrentQps() {
        long nanos = totalNanos.get();
        double seconds = nanos / 1_000_000_000.0;
        return seconds > 0.0 ? totalRows.get() / seconds : 0.0;
    }

    /**
     * 获取当前JOIN QPS值
     */
    public double getJoinQps() {
        long nanos = joinNanos.get();
        double seconds = nanos / 1_000_000_000.0;
        return seconds > 0.0 ? joinRows.get() / seconds : 0.0;
    }

    /**
     * 获取当前平均延迟（毫秒）
     */
    public double getAvgLatencyMs() {
        long batches = totalBatches.get();
        return batches > 0 ? (totalNanos.get() / 1_000_000.0) / batches : 0.0;
    }

    /**
     * 停止统计收集器
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * 更新最小/最大延迟
     */
    private void updateMinMax(long nanos) {
        long currentMin;
        do {
            currentMin = minBatchNanos.get();
            if (nanos >= currentMin) {
                break;
            }
        } while (!minBatchNanos.compareAndSet(currentMin, nanos));
        
        long currentMax;
        do {
            currentMax = maxBatchNanos.get();
            if (nanos <= currentMax) {
                break;
            }
        } while (!maxBatchNanos.compareAndSet(currentMax, nanos));
    }
    
    /**
     * 格式化数字（千分位）
     */
    private String formatNumber(long num) {
        if (num >= 1_000_000) {
            return String.format("%.1fM", num / 1_000_000.0);
        } else if (num >= 1_000) {
            return String.format("%.1fK", num / 1_000.0);
        }
        return String.valueOf(num);
    }

    // ========== 新增：多时间窗口统计 API ==========

    /**
     * 获取指定时间窗口的QPS
     * @param windowSeconds 窗口秒数：30, 60, 300, 600
     */
    public double getQpsForWindow(int windowSeconds) {
        return calculateQpsForWindow(windowSeconds);
    }

    /**
     * 获取30秒窗口QPS
     */
    public double getQps30s() {
        return calculateQpsForWindow(WINDOW_30_SEC);
    }

    /**
     * 获取1分钟窗口QPS
     */
    public double getQps1m() {
        return calculateQpsForWindow(WINDOW_1_MIN);
    }

    /**
     * 获取5分钟窗口QPS
     */
    public double getQps5m() {
        return calculateQpsForWindow(WINDOW_5_MIN);
    }

    /**
     * 获取10分钟窗口QPS
     */
    public double getQps10m() {
        return calculateQpsForWindow(WINDOW_10_MIN);
    }

    /**
     * 获取指定时间窗口的表TPS
     * @param table 表名
     * @param windowSeconds 窗口秒数
     */
    public double getTableTpsForWindow(String table, int windowSeconds) {
        Map<String, Double> tpsMap = calculateTableTpsForWindow(windowSeconds);
        return tpsMap.getOrDefault(table, 0.0);
    }

    /**
     * 获取指定时间窗口的平均延迟
     * @param windowSeconds 窗口秒数
     */
    public double getAvgLatencyForWindow(int windowSeconds) {
        return calculateAvgLatencyForWindow(windowSeconds);
    }

    /**
     * 获取最新JOIN查询行数
     */
    public long getLatestJoinRows() {
        return latestJoinRows;
    }

    /**
     * 获取最新JOIN查询延迟（毫秒）
     */
    public double getLatestJoinLatencyMs() {
        return latestJoinNanos / 1_000_000.0;
    }

    /**
     * 获取是否有最新JOIN统计
     */
    public boolean hasLatestJoin() {
        return hasLatestJoin;
    }

    /**
     * 获取完整统计数据（Map格式）
     */
    public Map<String, Object> getStatsMap() {
        Map<String, Object> stats = new HashMap<>();
        
        stats.put("totalRows", totalRows.get());
        stats.put("totalBatches", totalBatches.get());
        stats.put("totalSeconds", (System.nanoTime() - startNanos) / 1_000_000_000.0);
        stats.put("avgQps", getCurrentQps());
        
        if (rollingEnabled && totalSamples > 0) {
            Map<String, Double> qpsWindows = new HashMap<>();
            qpsWindows.put("30s", getQps30s());
            qpsWindows.put("1m", getQps1m());
            qpsWindows.put("5m", getQps5m());
            qpsWindows.put("10m", getQps10m());
            stats.put("qpsWindows", qpsWindows);
            
            Map<String, Double> latencyWindows = new HashMap<>();
            latencyWindows.put("30s", getAvgLatencyForWindow(WINDOW_30_SEC));
            latencyWindows.put("1m", getAvgLatencyForWindow(WINDOW_1_MIN));
            latencyWindows.put("5m", getAvgLatencyForWindow(WINDOW_5_MIN));
            latencyWindows.put("10m", getAvgLatencyForWindow(WINDOW_10_MIN));
            stats.put("latencyWindowsMs", latencyWindows);
            
            if (hasLatestJoin) {
                Map<String, Object> joinStats = new HashMap<>();
                joinStats.put("rows", getLatestJoinRows());
                joinStats.put("latencyMs", getLatestJoinLatencyMs());
                stats.put("latestJoin", joinStats);
            }
            
            if (tableStatsDetailed) {
                Map<String, Map<String, Double>> tableTps = new HashMap<>();
                for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                    Map<String, Double> windows = new HashMap<>();
                    windows.put("30s", getTableTpsForWindow(table, WINDOW_30_SEC));
                    windows.put("1m", getTableTpsForWindow(table, WINDOW_1_MIN));
                    windows.put("5m", getTableTpsForWindow(table, WINDOW_5_MIN));
                    windows.put("10m", getTableTpsForWindow(table, WINDOW_10_MIN));
                    tableTps.put(table, windows);
                }
                stats.put("tableTpsWindows", tableTps);
            }
        }
        
        return stats;
    }

    /**
     * 获取统计数据的JSON格式字符串
     */
    public String getStatsJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        
        sb.append("\"totalRows\":").append(totalRows.get()).append(",");
        sb.append("\"totalBatches\":").append(totalBatches.get()).append(",");
        sb.append("\"avgQps\":").append(String.format("%.2f", getCurrentQps()));
        
        if (rollingEnabled && totalSamples > 0) {
            sb.append(",\"qpsWindows\":{");
            sb.append("\"30s\":").append(String.format("%.2f", getQps30s())).append(",");
            sb.append("\"1m\":").append(String.format("%.2f", getQps1m())).append(",");
            sb.append("\"5m\":").append(String.format("%.2f", getQps5m())).append(",");
            sb.append("\"10m\":").append(String.format("%.2f", getQps10m()));
            sb.append("}");
            
            sb.append(",\"latencyWindowsMs\":{");
            sb.append("\"30s\":").append(String.format("%.2f", getAvgLatencyForWindow(WINDOW_30_SEC))).append(",");
            sb.append("\"1m\":").append(String.format("%.2f", getAvgLatencyForWindow(WINDOW_1_MIN))).append(",");
            sb.append("\"5m\":").append(String.format("%.2f", getAvgLatencyForWindow(WINDOW_5_MIN))).append(",");
            sb.append("\"10m\":").append(String.format("%.2f", getAvgLatencyForWindow(WINDOW_10_MIN)));
            sb.append("}");
            
            if (hasLatestJoin) {
                sb.append(",\"latestJoin\":{");
                sb.append("\"rows\":").append(getLatestJoinRows()).append(",");
                sb.append("\"latencyMs\":").append(String.format("%.3f", getLatestJoinLatencyMs()));
                sb.append("}");
            }
            
            if (tableStatsDetailed) {
                sb.append(",\"tableTps\":{");
                for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                    sb.append("\"").append(table).append("\":{");
                    sb.append("\"30s\":").append(String.format("%.2f", getTableTpsForWindow(table, WINDOW_30_SEC))).append(",");
                    sb.append("\"1m\":").append(String.format("%.2f", getTableTpsForWindow(table, WINDOW_1_MIN))).append(",");
                    sb.append("\"5m\":").append(String.format("%.2f", getTableTpsForWindow(table, WINDOW_5_MIN))).append(",");
                    sb.append("\"10m\":").append(String.format("%.2f", getTableTpsForWindow(table, WINDOW_10_MIN)));
                    sb.append("},");
                }
                sb.setLength(sb.length() - 1);
                sb.append("}");
            }
        }
        
        sb.append("}");
        return sb.toString();
    }
}

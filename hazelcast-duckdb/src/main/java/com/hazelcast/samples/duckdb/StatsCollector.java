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
    private final Map<String, long[]> deltaTableNanos = new ConcurrentHashMap<>();
    
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
        
        deltaTableNanos.put("buyer_info", new long[MAX_SAMPLES]);
        deltaTableNanos.put("order_main", new long[MAX_SAMPLES]);
        deltaTableNanos.put("order_item", new long[MAX_SAMPLES]);
        
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
        
        // 更新延迟统计
        updateMinMax(nanos);
        sumBatchNanos.addAndGet(nanos);
        
        // 记录延迟样本用于百分位计算
        recordLatencySample(nanos);
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
        deltaTableNanos.values().forEach(arr -> Arrays.fill(arr, 0L));
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
        
        // 使用ANSI颜色美化输出
        String ANSI_RESET = "\u001B[0m";
        String ANSI_BLUE = "\u001B[34m";
        String ANSI_GREEN = "\u001B[32m";
        String ANSI_YELLOW = "\u001B[33m";
        String ANSI_CYAN = "\u001B[36m";
        
        System.out.println("\n" + "=".repeat(70));
        System.out.println(ANSI_BLUE + "               DuckDB 写入性能测试汇总统计" + ANSI_RESET);
        System.out.println("=".repeat(70));
        
        // 总体吞吐统计
        System.out.println("\n" + ANSI_GREEN + "【总体吞吐统计】" + ANSI_RESET);
        System.out.printf("  %-20s %,d%n", "总批次数:", batches);
        System.out.printf("  %-20s %,d%n", "总输出行数:", rows);
        System.out.printf("  %-20s %.3f 秒%n", "总耗时:", seconds);
        System.out.printf("  %-20s " + ANSI_YELLOW + "%.1f" + ANSI_RESET + " (行/秒)%n", "QPS:", qps);
        System.out.printf("  %-20s %.2f 批/秒%n", "批次吞吐:", batchesPerSecond);
        
        // 延迟统计
        System.out.println("\n" + ANSI_GREEN + "【延迟统计 (毫秒)】" + ANSI_RESET);
        System.out.printf("  %-20s %.3f%n", "平均延迟:", percentiles[0]);
        System.out.printf("  %-20s %.3f%n", "最小延迟:", minBatchMs);
        System.out.printf("  %-20s %.3f%n", "最大延迟:", maxBatchMs);
        System.out.printf("  %-20s " + ANSI_CYAN + "P50: %.3f | P90: %.3f | P95: %.3f | P99: %.3f" + ANSI_RESET + "%n", 
                "延迟百分位:", percentiles[1], percentiles[2], percentiles[3], percentiles[4]);
        
        // 重试统计
        System.out.println("\n" + ANSI_GREEN + "【重试统计】" + ANSI_RESET);
        System.out.printf("  %-20s %,d%n", "总重试次数:", totalRetries.get());
        double retryRate = batches > 0 ? (double) totalRetries.get() / batches * 100 : 0.0;
        System.out.printf("  %-20s %.4f%%%n", "重试率:", retryRate);
        
        // 按表统计
        if (tableStatsDetailed) {
            System.out.println("\n" + ANSI_GREEN + "【按表写入统计】" + ANSI_RESET);
            
            // 直接遍历三个表
            for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                long tableRowCount = tableRows.getOrDefault(table, new AtomicLong()).get();
                long tableNanosValue = tableNanos.getOrDefault(table, new AtomicLong()).get();
                long tableRetryCount = tableRetries.getOrDefault(table, new AtomicLong()).get();
                double tableSeconds = tableNanosValue / 1_000_000_000.0;
                double tableQps = tableSeconds > 0.0 ? tableRowCount / tableSeconds : 0.0;
                double avgRowMs = tableRowCount > 0 ? (tableNanosValue / 1_000_000.0) / tableRowCount : 0.0;
                double tableRetryRate = tableRowCount > 0 ? (double) tableRetryCount / tableRowCount * 100 : 0.0;
                
                System.out.printf("  ├─ " + ANSI_YELLOW + "%s" + ANSI_RESET + ":%n", table);
                System.out.printf("  │   %-16s %,d%n", "行数:", tableRowCount);
                System.out.printf("  │   %-16s %.1f%n", "QPS:", tableQps);
                System.out.printf("  │   %-16s %.4f 毫秒%n", "平均每行延迟:", avgRowMs);
                System.out.printf("  │   %-16s %,d (%.4f%%)%n", "重试次数:", tableRetryCount, tableRetryRate);
            }
        }
        
        // JOIN 查询统计
        long joinRowCount = joinRows.get();
        long joinNanosValue = joinNanos.get();
        double joinSeconds = joinNanosValue / 1_000_000_000.0;
        double joinQps = joinSeconds > 0.0 ? joinRowCount / joinSeconds : 0.0;
        
        System.out.println("\n" + ANSI_GREEN + "【宽表查询 (Join) 统计】" + ANSI_RESET);
        System.out.printf("  %-20s %,d%n", "返回行数:", joinRowCount);
        System.out.printf("  %-20s %.1f%n", "QPS:", joinQps);
        System.out.printf("  %-20s %.3f 秒%n", "总耗时:", joinSeconds);
        
        System.out.println("\n" + "=".repeat(70));
    }

    /**
     * 打印实时统计（支持多时间窗口）
     */
    public void printStats() {
        if (!rollingEnabled) {
            return;
        }
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
                sb.append(String.format("%-18s", "最近1s: " + formatNumber(deltaRows) + "行"));
                
                // 多时间窗口总体 QPS
                sb.append(String.format("| QPS[30s/1m/5m/10m]: %.0f/%.0f/%.0f/%.0f ", 
                        qps30Sec, qps1Min, qps5Min, qps10Min));
                
                // 各表 TPS（显示4个时间窗口）
                if (tableStatsDetailed) {
                    sb.append("| ");
                    for (String table : new String[]{"buyer_info", "order_main", "order_item"}) {
                        double tps30 = tableTps30Sec.getOrDefault(table, 0.0);
                        double tps60 = tableTps1Min.getOrDefault(table, 0.0);
                        double tps300 = tableTps5Min.getOrDefault(table, 0.0);
                        double tps600 = tableTps10Min.getOrDefault(table, 0.0);
                        sb.append(table).append("[30s/1m/5m/10m]:").append(String.format("%.0f/%.0f/%.0f/%.0f", tps30, tps60, tps300, tps600)).append(" ");
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
     * 计算指定时间窗口的总体 QPS
     */
    private double calculateQpsForWindow(int windowSize) {
        if (totalSamples == 0) {
            return 0.0;
        }
        
        int actualSamples = Math.min(windowSize, totalSamples);
        long totalRowsInWindow = 0;
        
        // 从循环缓冲区中读取最近 N 个样本
        for (int i = 0; i < actualSamples; i++) {
            int idx = (sampleIndex - 1 - i + MAX_SAMPLES) % MAX_SAMPLES;
            if (sampleTimestamps[idx] > 0) {
                totalRowsInWindow += deltaTotalRows[idx];
            }
        }
        
        // 计算窗口时间（秒）
        double windowSeconds = actualSamples;
        
        return windowSeconds > 0 ? totalRowsInWindow / windowSeconds : 0.0;
    }
    
    /**
     * 计算指定时间窗口的各表 TPS
     */
    private Map<String, Double> calculateTableTpsForWindow(int windowSize) {
        Map<String, Double> result = new HashMap<>();
        
        if (totalSamples == 0) {
            return result;
        }
        
        int actualSamples = Math.min(windowSize, totalSamples);
        
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
            
            double windowSeconds = actualSamples;
            double tps = windowSeconds > 0 ? totalRowsInWindow / windowSeconds : 0.0;
            result.put(table, tps);
        }
        
        return result;
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
}

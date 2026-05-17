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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能统计收集器 - 支持准确的实时统计和友好的输出格式
 */
public final class StatsCollector {

    private static final StatsCollector INSTANCE = new StatsCollector();

    // ==================== 累计统计 ====================
    
    private final AtomicLong totalBatches = new AtomicLong();
    private final AtomicLong totalRows = new AtomicLong();
    private final AtomicLong totalNanos = new AtomicLong();
    private final AtomicLong totalRetries = new AtomicLong();
    private final Map<String, AtomicLong> tableRows = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> tableNanos = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> tableRetries = new ConcurrentHashMap<>();
    private final AtomicLong joinRows = new AtomicLong();
    private final AtomicLong joinNanos = new AtomicLong();
    
    // ==================== 延迟统计（用于计算百分位） ====================
    
    private final AtomicLong minBatchNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxBatchNanos = new AtomicLong(0);
    private final AtomicLong sumBatchNanos = new AtomicLong(0);
    private final AtomicLong countBatchNanos = new AtomicLong(0);
    
    // 滑动窗口延迟采样（用于计算百分位）
    private static final int LATENCY_WINDOW_SIZE = 1024;
    private final long[] latencySamples = new long[LATENCY_WINDOW_SIZE];
    private volatile int latencyIndex = 0;
    
    // ==================== 滚动统计 ====================
    
    private final int windowSeconds;
    private final long[] sampleTimestamps;
    private final long[] sampleTotalRows;
    private final Map<String, long[]> sampleTableRows = new ConcurrentHashMap<>();
    private final long[] sampleJoinRows;
    private int sampleIndex = 0;
    private final ScheduledExecutorService scheduler;
    private final boolean rollingEnabled;
    private final boolean tableStatsDetailed;
    
    // 上一次采样的值（用于计算增量）
    private volatile long lastTotalRows = 0;
    private volatile long lastJoinRows = 0;
    private final Map<String, AtomicLong> lastTableRows = new ConcurrentHashMap<>();

    private StatsCollector() {
        // 初始化表统计
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
        
        // 读取配置
        rollingEnabled = PerfConfig.ROLLING_STATS_ENABLED;
        windowSeconds = PerfConfig.ROLLING_STATS_WINDOW_SECONDS;
        tableStatsDetailed = PerfConfig.TABLE_STATS_DETAILED;
        
        // 初始化滚动统计窗口
        sampleTimestamps = new long[windowSeconds];
        sampleTotalRows = new long[windowSeconds];
        sampleJoinRows = new long[windowSeconds];
        
        // 启动滚动统计线程
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
     * 记录 JOIN 查询统计
     */
    public void recordJoin(long rows, long nanos) {
        joinRows.addAndGet(rows);
        joinNanos.addAndGet(nanos);
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
        
        Arrays.fill(sampleTimestamps, 0L);
        Arrays.fill(sampleTotalRows, 0L);
        Arrays.fill(sampleJoinRows, 0L);
        Arrays.fill(latencySamples, 0L);
        sampleTableRows.clear();
        sampleIndex = 0;
        
        lastTotalRows = 0;
        lastJoinRows = 0;
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
            List.of("buyer_info", "order_main", "order_item").forEach(table -> {
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
            });
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
     * 打印实时统计（类似HighPerformanceDataGenerator的printStats方法）
     * 用于在数据处理流程中打印DuckDB写入和JOIN查询性能指标
     */
    public void printStats() {

        if (PerfConfig.ROLLING_STATS_ENABLED) {
            return;
        }

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
        
        // 计算延迟百分位
        double[] percentiles = calculateLatencyPercentiles();
        
        System.out.printf("[DUCKDB] QPS: %.1f | 总批次: %,d | 总行数: %,d | 平均延迟: %.2fms (P50: %.2fms P99: %.2fms) | joinSeconds: %.2fms | joinRowCount: %,d | JOIN-QPS: %.1f | 表写入: buyer=%d(%.1fTPS) order=%d(%.1fTPS) item=%d(%.1fTPS)%n",
                qps, batches, rows, avgLatencyMs, percentiles[1], percentiles[4], joinSeconds, joinRowCount, joinQps,
                buyerRows, buyerTps, orderRows, orderTps, itemRows, itemTps);
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
     * 采样并打印滚动统计
     */
    private void sampleAndMaybePrint() {
        try {
            long now = System.currentTimeMillis();
            long currentTotalRows = totalRows.get();
            long currentJoinRows = joinRows.get();
            
            // 计算1秒增量
            long deltaTotalRows = currentTotalRows - lastTotalRows;
            long deltaJoinRows = currentJoinRows - lastJoinRows;
            
            // 更新采样窗口
            sampleTimestamps[sampleIndex] = now;
            sampleTotalRows[sampleIndex] = currentTotalRows;
            sampleJoinRows[sampleIndex] = currentJoinRows;
            
            // 记录各表的采样数据
            if (tableStatsDetailed) {
                tableRows.forEach((table, value) -> 
                    sampleTableRows.computeIfAbsent(table, ignored -> new long[windowSeconds])[sampleIndex] = value.get()
                );
            }
            
            int currentIndex = sampleIndex;
            sampleIndex = (sampleIndex + 1) % windowSeconds;
            int prevIndex = (currentIndex + windowSeconds - 1) % windowSeconds;
            int oldestIndex = (currentIndex + 1) % windowSeconds;
            
            // 计算窗口平均（使用实际时间差）
            long windowStartTime = sampleTimestamps[oldestIndex];
            double windowDurationSeconds = windowStartTime > 0 ? (now - windowStartTime) / 1000.0 : windowSeconds;
            long windowRows = currentTotalRows - sampleTotalRows[oldestIndex];
            double avgWindowQps = windowDurationSeconds > 0 ? windowRows / windowDurationSeconds : 0.0;
            
            // 更新上次采样值
            lastTotalRows = currentTotalRows;
            lastJoinRows = currentJoinRows;
            
            // 打印实时统计（只有当有有效数据时）
            if (rollingEnabled && sampleTimestamps[prevIndex] != 0L) {
                StringBuilder sb = new StringBuilder();
                sb.append("\r");
                sb.append("[实时] ");
                sb.append(String.format("%-20s", "最近1秒: " + formatNumber(deltaTotalRows) + "行"));
                sb.append(String.format("%-25s", "| 最近" + windowSeconds + "秒平均: " + formatNumber((long) avgWindowQps) + " QPS"));
                
                // 打印各表统计
                if (tableStatsDetailed) {
                    sb.append("| ");
                    List.of("buyer_info", "order_main", "order_item").forEach(table -> {
                        long currentTableRows = tableRows.getOrDefault(table, new AtomicLong()).get();
                        long lastTableRowValue = lastTableRows.getOrDefault(table, new AtomicLong()).get();
                        long deltaTableRows = currentTableRows - lastTableRowValue;
                        lastTableRows.get(table).set(currentTableRows);
                        
                        sb.append(table).append(":").append(formatNumber(deltaTableRows)).append(" ");
                    });
                }
                
                sb.append("| JOIN: ").append(formatNumber(deltaJoinRows)).append("行");
                
                // 添加延迟统计（最近的平均延迟）
                long avgLatencyNanos = countBatchNanos.get() > 0 ? sumBatchNanos.get() / countBatchNanos.get() : 0;
                sb.append(String.format(" | 延迟: %.2fms", avgLatencyNanos / 1_000_000.0));
                
                System.out.print(sb);
                System.out.flush();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
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

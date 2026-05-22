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
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * DuckDB + MongoDB Join 查询性能优化示例
 * 
 * 包含除索引外的多种优化策略
 */
public class DuckDbMongoJoinOptimization {

    private static final String MONGO_SCHEMA = "mongo_db";
    private static final String MONGO_DB = "tapdata-develop";
    private static final String BUYER_COLLECTION = "buyer";
    private static final String ORDER_COLLECTION = "order";

    public static void main(String[] args) {
        System.out.println("=== DuckDB + MongoDB Join 查询性能优化 ===\n");
        
        String duckDbUrl = "jdbc:duckdb:memory:";
        
        try (Connection conn = DriverManager.getConnection(duckDbUrl);
             Statement stmt = conn.createStatement()) {
            
            // 1. 安装和加载扩展
            installAndLoadExtensions(stmt);
            
            // 2. 设置 DuckDB 性能参数
            setPerformanceParameters(stmt);
            
            // 3. 连接 MongoDB
            String mongoConnStr = "mongodb://localhost:27017/" + MONGO_DB;
            attachMongoDB(stmt, mongoConnStr);
            
            // 4. 各种优化策略演示
            System.out.println("\n--- 优化策略演示 ---");
            
            // 策略1: 本地缓存
            demoLocalCaching(stmt);
            
            // 策略2: 投影裁剪 + 谓词下推
            demoPredicatePushdown(stmt);
            
            // 策略3: 物化视图
            demoMaterializedView(stmt);
            
            // 策略4: 查询计划分析
            demoQueryPlanAnalysis(stmt);
            
            System.out.println("\n=== 优化演示完成 ===");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 安装并加载必要的 DuckDB 扩展
     */
    private static void installAndLoadExtensions(Statement stmt) throws Exception {
        System.out.println("步骤1: 安装 DuckDB 扩展");
        
        // 安装 MongoDB 扩展
        tryExecute(stmt, "INSTALL mongo FROM 'http://community-extensions.duckdb.org';", 
                   "安装Mongo扩展");
        tryExecute(stmt, "LOAD mongo;", "加载Mongo扩展");
        
        // 安装 HTTPFS 扩展（用于读取 S3/本地文件）
        tryExecute(stmt, "INSTALL httpfs;", "安装HTTPFS扩展");
        tryExecute(stmt, "LOAD httpfs;", "加载HTTPFS扩展");
        
        System.out.println();
    }
    
    /**
     * 设置 DuckDB 性能参数
     */
    private static void setPerformanceParameters(Statement stmt) throws Exception {
        System.out.println("步骤2: 设置性能参数");
        
        // 通过 SQL 设置
        stmt.execute("SET threads = " + Runtime.getRuntime().availableProcessors() + ";");
        stmt.execute("SET max_memory = '2GB';");
        stmt.execute("SET enable_hashjoin = true;");
        stmt.execute("SET enable_nestloop = true;");

        System.out.println("✅ 性能参数已设置");
        System.out.println();
    }
    
    /**
     * 连接 MongoDB
     */
    private static void attachMongoDB(Statement stmt, String mongoConnStr) throws Exception {
        System.out.println("步骤3: 连接 MongoDB");
        
        String attachSql = String.format(
            "ATTACH '%s' AS mongo_db (TYPE MONGO);",
            mongoConnStr
        );
        
        tryExecute(stmt, attachSql, "连接 MongoDB");
        System.out.println();
    }
    
    /**
     * 策略1: 本地缓存优化
     */
    private static void demoLocalCaching(Statement stmt) throws Exception {
        System.out.println("\n优化策略1: 本地缓存优化");

        // 检查并缓存 buyers 表
        System.out.println("  创建本地缓存表");
        try {
            stmt.execute("DROP TABLE IF EXISTS local_buyers");
            stmt.execute("DROP TABLE IF EXISTS local_orders");
            stmt.execute("DROP TABLE IF EXISTS local_order_items");

            // 使用 CTAS (CREATE TABLE AS SELECT) 缓存
            // 只缓存需要的字段，减少内存使用
            stmt.execute(String.format(
                    "CREATE TABLE local_buyers AS "
                            + "SELECT buyer_id, buyer_name, email, status "
                            + "FROM %s",
                    mongoTable(BUYER_COLLECTION)));

            stmt.execute(String.format(
                    "CREATE TABLE local_orders AS "
                            + "SELECT order_id, buyer_id, order_date, total_amount "
                            + "FROM %s",
                    mongoTable(ORDER_COLLECTION)));

            // 创建本地索引加速 Join
            stmt.execute("CREATE INDEX idx_local_buyers_id ON local_buyers(buyer_id)");
            stmt.execute("CREATE INDEX idx_local_orders_buyer_id ON local_orders(buyer_id)");

            System.out.println("  ✅ 本地缓存创建完成");

            // 使用本地缓存进行 Join 查询
            System.out.println("  执行本地缓存 Join 查询");
            long start = System.currentTimeMillis();

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT b.buyer_name, COUNT(o.order_id) as order_count, "
                            + "SUM(o.total_amount) as total_spent "
                            + "FROM local_buyers b "
                            + "JOIN local_orders o ON b.buyer_id = o.buyer_id "
                            + "WHERE b.status = 'ACTIVE' "
                            + "GROUP BY b.buyer_name "
                            + "ORDER BY total_spent DESC "
                            + "LIMIT 10")) {
                long end = System.currentTimeMillis();
                System.out.println("  ⏱️  查询耗时: " + (end - start) + "ms");

                System.out.println("  🏆 结果:");
                while (rs.next()) {
                    System.out.printf("    %s: %d 订单, $%.2f%n",
                            rs.getString(1),
                            rs.getInt(2),
                            rs.getDouble(3));
                }
            }

        } catch (Exception e) {
            System.out.println("  ⚠️  演示跳过 (集合不存在): " + e.getMessage());
        }
    }
    
    /**
     * 策略2: 投影裁剪 + 谓词下推优化
     */
    private static void demoPredicatePushdown(Statement stmt) throws Exception {
        System.out.println("\n优化策略2: 投影裁剪 + 谓词下推");
        
        System.out.println("  对比两种查询方式:");
        
        System.out.println("\n  方式1: 不优化（全量读取后处理）");
        System.out.println("  SQL: SELECT * FROM table1 JOIN table2 ON ...");
        
        System.out.println("\n  方式2: 优化后（谓词下推 + 投影裁剪）");
        System.out.println("  SQL: ");
        System.out.println("    SELECT t1.id, t1.name, t2.amount");
        System.out.println("    FROM (SELECT id, name FROM mongo.table WHERE status = 'ACTIVE') t1");
        System.out.println("    JOIN (SELECT t1_id, amount FROM mongo.table2 WHERE amount > 100) t2");
        System.out.println("    ON t1.id = t2.t1_id");
        
        // 尝试执行优化查询
        try {
            long start = System.currentTimeMillis();
            String sql = String.format(
                    "SELECT b.buyer_id, b.buyer_name, o.order_id, o.total_amount "
                            + "FROM (SELECT buyer_id, buyer_name FROM %s WHERE status = 'ACTIVE') b "
                            + "JOIN (SELECT order_id, buyer_id, total_amount FROM %s WHERE total_amount > 100) o "
                            + "ON b.buyer_id = o.buyer_id "
                            + "LIMIT 5",
                    mongoTable(BUYER_COLLECTION),
                    mongoTable(ORDER_COLLECTION));
            stmt.executeQuery(sql).close();
            long end = System.currentTimeMillis();
            System.out.println("  ⏱️  优化查询耗时: " + (end - start) + "ms");
        } catch (Exception e) {
            System.out.println("  ⚠️  演示跳过: " + e.getMessage());
        }
    }
    
    /**
     * 策略3: 物化视图优化
     */
    private static void demoMaterializedView(Statement stmt) throws Exception {
        System.out.println("\n优化策略3: 物化视图优化");
        
        System.out.println("  创建物化视图（预先计算 Join 结果）");
        
        try {
            stmt.execute("DROP VIEW IF EXISTS mv_buyer_orders");
            
            stmt.execute(String.format(
                    "CREATE OR REPLACE MATERIALIZED VIEW mv_buyer_orders AS "
                            + "SELECT b.buyer_id, b.buyer_name, o.order_id, "
                            + "o.order_date, o.total_amount "
                            + "FROM local_buyers b "
                            + "JOIN local_orders o ON b.buyer_id = o.buyer_id"));

            System.out.println("  ✅ 物化视图创建完成");

            System.out.println("  使用物化视图查询（秒级响应）");
            long start = System.currentTimeMillis();
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM mv_buyer_orders "
                            + "WHERE total_amount > 500 "
                            + "ORDER BY order_date DESC "
                            + "LIMIT 10")) {
                long end = System.currentTimeMillis();
                System.out.println("  ⏱️  物化视图查询耗时: " + (end - start) + "ms");
                while (rs.next()) {
                    // consume results for timing
                }
            }

            System.out.println("\n  刷新物化视图（当源数据变化时）");
            stmt.execute("REFRESH MATERIALIZED VIEW mv_buyer_orders");
            System.out.println("  ✅ 物化视图刷新完成");

        } catch (Exception e) {
            System.out.println("  ⚠️  演示跳过: " + e.getMessage());
        }
    }
    
    /**
     * 策略4: 查询计划分析与调优
     */
    private static void demoQueryPlanAnalysis(Statement stmt) throws Exception {
        System.out.println("\n优化策略4: 查询计划分析");
        
        System.out.println("  使用 EXPLAIN ANALYZE 查看查询计划和性能");
        
        try (ResultSet rs = stmt.executeQuery(
                "EXPLAIN ANALYZE "
                        + "SELECT b.buyer_name, COUNT(o.order_id) as order_count "
                        + "FROM local_buyers b "
                        + "JOIN local_orders o ON b.buyer_id = o.buyer_id "
                        + "GROUP BY b.buyer_name")) {
            System.out.println("\n  📊 查询计划:");
            while (rs.next()) {
                System.out.println("    " + rs.getString(1));
            }
        } catch (Exception e) {
            System.out.println("  ⚠️  演示跳过: " + e.getMessage());
        }
    }

    private static String mongoTable(String collection) {
        return String.format("%s.\"%s\".\"%s\"", MONGO_SCHEMA, MONGO_DB, collection);
    }

    private static boolean tryExecute(Statement stmt, String sql, String label) {
        try {
            boolean hasResult = stmt.execute(sql);
            System.out.println("  ✅ " + label + " - 执行成功");
            return true;
        } catch (Exception e) {
            System.out.println("  ⚠️  " + label + " - 跳过: " + e.getMessage());
            return false;
        }
    }
}

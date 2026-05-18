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

public class DuckDbMongoOfficialDemo {
    public static void main(String[] args) {
        // 官方推荐连接参数：设置内存限制，避免 OOM
        String duckDbUrl = "jdbc:duckdb:memory:";

        try (Connection conn = DriverManager.getConnection(duckDbUrl);
             Statement stmt = conn.createStatement()) {

            // 步骤1：查询扩展支持的版本/来源
            printExtensionInfo(stmt, "mongo");

            // 步骤2：安装 mongo 扩展（仅首次执行，会自动下载）
            stmt.execute("INSTALL mongo FROM 'http://community-extensions.duckdb.org';");

            // 步骤3：加载 mongo 扩展（每次连接必须执行）
            stmt.execute("LOAD mongo;");

            // 步骤4：ATTACH MongoDB 服务（官方核心语法）
            // 支持标准 MongoDB 连接串格式，包括认证和 Atlas 集群
//            String mongoConnStr = "mongodb://admin:123456@localhost:27017"; // 官方示例格式
            String mongoConnStr = "mongodb://localhost:27017/tapdata-develop"; // 官方示例格式
            String attachSql = String.format("ATTACH '%s' AS mongo_db (TYPE MONGO);", mongoConnStr);
            stmt.execute(attachSql); // 官方推荐方式

            // 步骤5：直接查询 MongoDB 外部表（官方查询语法）
            // 格式：SELECT * FROM 别名.数据库名.集合名
            String querySql = "SELECT _id, category, hot_reloading FROM mongo_db.\"tapdata-develop\".Settings LIMIT 10";
            executeAndPrint(stmt, querySql);

            // 步骤6：（官方推荐高性能方案）导入内部表缓存数据
            String cacheSql = """
                CREATE TABLE IF NOT EXISTS local_users AS
                SELECT _id, category, hot_reloading FROM mongo_db.Settings LIMIT 10
            """;
            executeAndPrint(stmt, cacheSql);
            executeAndPrint(stmt, "SELECT * FROM local_users;");

            // 步骤7：（可选）清除 MongoDB schema 缓存（官方函数）：MongoDB 集合的【结构（Schema）变了】的时候执行！
            executeAndPrint(stmt, "SELECT * FROM mongo_clear_cache();");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printExtensionInfo(Statement stmt, String extensionName) throws Exception {
        String sql = "SELECT extension_name, extension_version, installed_from, install_mode "
                + "FROM duckdb_extensions()";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String name = rs.getString("extension_name");
                if (!extensionName.equalsIgnoreCase(name)) {
                    continue;
                }
                System.out.printf("扩展: %s | 版本: %s | 来源: %s | 模式: %s%n",
                        name,
                        rs.getString("extension_version"),
                        rs.getString("installed_from"),
                        rs.getString("install_mode"));
            }
        }
    }

    private static void executeAndPrint(Statement stmt, String sql) throws Exception {
        boolean hasResultSet = stmt.execute(sql);
        System.out.println("执行SQL: " + sql.replace("\n", " ").trim());
        if (!hasResultSet) {
            System.out.println("影响行数: " + stmt.getUpdateCount());
            return;
        }
        try (ResultSet rs = stmt.getResultSet()) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        row.append(" | ");
                    }
                    row.append(rs.getMetaData().getColumnLabel(i)).append("=").append(rs.getString(i));
                }
                System.out.println(row);
            }
        }
    }
}
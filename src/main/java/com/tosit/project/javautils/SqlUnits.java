package com.tosit.project.javautils;

/**
 * sql语句工具类
 * <p>
 * Created by Wanghan on 2017/3/13.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class SqlUnits {

    /**
     * 拼接sql查询语句
     *
     * @param totalSql
     * @param currentSql
     * @return
     */
    public static String concatSQL(String totalSql, String currentSql) {
        StringBuilder sqlBuilder = new StringBuilder(currentSql);
        sqlBuilder.insert(currentSql.length(), ")");
        sqlBuilder.insert(0, "(");

        // 加入where
        totalSql = trimSpace(totalSql);
        if (!totalSql.contains("WHERE")) {
            totalSql += " WHERE ";
            sqlBuilder.insert(0,totalSql);
        }else {
            sqlBuilder.insert(0, totalSql + " AND ");
        }

        return sqlBuilder.toString();
    }

    /**
     * 去除sql查询语句多出的or
     *
     * @param sql
     * @return
     */
    public static String trimOr(String sql) {
        // 去除行尾空格
        sql = trimSpace(sql);
        if (sql.endsWith("OR") || sql.endsWith("or")) {
            sql = sql.substring(0, sql.length() - 2);
        }

        return sql;
    }

    /**
     * 去除sql查询语句行首行尾空格
     *
     * @param sql
     * @return
     */
    public static String trimSpace(String sql) {
        //  去除行尾空格
        while (sql.endsWith(" ")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        while (sql.startsWith(" ")) {
            sql = sql.substring(1);
        }

        return sql;
    }
}

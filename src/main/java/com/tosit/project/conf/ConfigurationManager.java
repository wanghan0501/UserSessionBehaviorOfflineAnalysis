package com.tosit.project.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载管理类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */

public class ConfigurationManager {

    // 配置属性
    private static Properties properties = new Properties();

    static {
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取String型配置项
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    /**
     * 获取Integer型配置项
     *
     * @param key
     * @return
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取Boolean型配置项
     *
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long型配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 获取Double型配置项
     *
     * @param key
     * @return
     */
    public static Double getDouble(String key) {
        String value = getProperty(key);
        try {
            return Double.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0.0D;
    }
}

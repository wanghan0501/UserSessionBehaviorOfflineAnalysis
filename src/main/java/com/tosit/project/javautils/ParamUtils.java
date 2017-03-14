package com.tosit.project.javautils;

import com.tosit.project.conf.ConfigurationManager;
import com.tosit.project.constants.Constants;
import com.tosit.project.exception.ParameterException;
import org.json.JSONArray;
import org.json.JSONObject;
/**
 * 解析参数工具类
 * <p>
 * Created by Wanghan on 2017/3/12.
 * Copyright © Wanghan SCU. All Rights Reserved
 */
public class ParamUtils {
    /**
     * 获取用户提交的spark任务ID
     *
     * @param args
     * @param taskType
     * @return
     */
    public static Long getTaskIdFromArgs(String[] args, String taskType) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {

        }

        return 1L;
    }

    /**
     * 获得json某一关键字对应的一个值
     * 例如{"key":["value1"]},返回String类型的value1
     *
     * @param json
     * @param key
     * @return
     * @throws ParameterException
     */
    public static String getSingleValue(JSONObject json, String key) throws ParameterException {
        // 输入的key非法
        if (key == null || key.equals("")) throw new ParameterException();
        // 处理key不存在的异常
        try {
            JSONArray jsonArray = json.getJSONArray(key);
            if (jsonArray != null && jsonArray.length() > 0) {
                return jsonArray.getString(0);
            }
        } catch (Exception e) {
            System.out.println(key + " doesn't exist.");
        }

        return null;
    }

    /**
     * 获得json某一关键字对应的多个值
     * 例如{"key":["value1","value2"]},返回String数组["value1,"value2"]
     *
     * @param json
     * @param key
     * @return
     * @throws ParameterException
     */
    public static String[] getMultipleValues(JSONObject json, String key) throws ParameterException {
        // 输入的key非法
        if (key == null || key.equals("")) throw new ParameterException();
        // 处理key不存在的异常
        try {
            JSONArray jsonArray = json.getJSONArray(key);
            int jsonLength = jsonArray.length();
            String[] jsonValues = new String[jsonLength];
            for (int i = 0; i < jsonLength; i++) {
                jsonValues[i] = jsonArray.getString(i);
            }
            return jsonValues;
        } catch (Exception e) {
            System.out.println(key + " doesn't exist.");
        }

        return null;
    }

}

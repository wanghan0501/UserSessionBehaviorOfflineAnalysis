package com.tosit.project.javautils;

import com.tosit.project.conf.ConfigurationManager;
import com.tosit.project.constants.Constants;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by Wanghan on 2017/3/11.
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
     * 从json对象中提取任务参数
     *
     * @param json
     * @param field
     * @return
     */
    public static String getParam(JSONObject json, String field) {
        JSONArray jsonArray = json.getJSONArray(field);
        if (jsonArray != null && jsonArray.length() > 0) {

            return jsonArray.getString(0);
        }
        return null;
    }
}

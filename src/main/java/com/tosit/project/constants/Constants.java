package com.tosit.project.constants;

/**
 * 配置常量接口类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */

public interface Constants {

    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String SPARK_MASTER = "local";
    String SPARK_LOCAL = "spark.local";

    String TABLE_USER_VISIT_ACTION = "user_visit_action";
    String TABLE_USER_INFO = "user_info";
    String TABLE_PRODUCT_INFO = "product_info";

    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEYWORDS = "searchKeyWords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_AGE = "age";

    String JDBC_URL = "jdbc.driver";
    String JDBC_USER = "jdbc.driver";
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_PASSWORD = "jdbc.driver";
    String JDBC_USER_PROD = "";
    String JDBC_URL_PROD = "";
    String JDBC_PASSWORD_PROD = "";

    String DBC_DATASOURCE_SIZE = "jdbc.datasource.size";

    String LOCAL_SESSION_TASKID = "spark.local.taskid.session";
    String LOCAL_SESSION_DATA_PATH = "spark.local.session.data.path";
    String LOCAL_USER_DATA_PATH = "spark.local.user.data.path";
    String LOCAL_PRODUCT_DATA_PATH = "spark.local.product.data.path";

    /**
     * json 相关参数
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITYS = "citys";
    String PARAM_SEX = "sex";
    String PARAM_SEARCH_WORDS = "searchWords";
    String PARAM_CATEGORY_IDS = "categoryIds";
    String PARAM_USER_ID = "user_id";
    String PARAM_SESSION_ID = "session_id";

    // 分割符
    String VALUE_SEPARATOR = "|";
    String REGULAR_VALUE_SEPARATOR = "\\|";

    /**
     * 聚合统计业务计算相关变量
     */
    String SESSION_COUNT = "session_count";

    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";


}
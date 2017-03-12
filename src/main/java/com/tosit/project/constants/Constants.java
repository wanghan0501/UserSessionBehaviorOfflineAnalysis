package com.tosit.project.constants;

/**
 * 配置常量接口类
 * <p>
 * Created by Wanghan on 2017/3/11.
 * Copyright © Wanghan SCU. All Rights Reserved
 */

public interface Constants {

    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String SPARK_MASTER = "local[2]";
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
    String FILED_VISIT_LENGTH = "visitLength";
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

    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";

    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITYS = "citys";
    String PARAM_SEX = "sex";
    String PARAM_SEARCH_WORDS = "searchWords";
    String PARAM_CATEGORY_IDS = "categoryIds";

    // 表结构
    String PARAM_USER_ID = "user_id";
    String PARAM_SESSION_ID = "session_id";

    // 分割符
    String VALUE_SEPARATOR = "|";
    String REGULAR_VALUE_SEPARATOR = "\\|";

}
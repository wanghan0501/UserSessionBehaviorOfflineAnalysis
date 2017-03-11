package com.tosit.project.constants;

/**
 * Created by mac on 2017/3/11.
 */

public interface Constants {

    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
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

    String SPARK_LOCAL = "spark.local";
    String DBC_DATASOURCE_SIZE = "jdbc.datasource.size";

    String SPARK_LOCAL_SESSION_TASKID = "spark.local.taskid.session";
    String LOCAL_SESSION_DATA_PATH = "spark.local.session.data.path";
    String LOCAL_USER_DATA_PATH = "spark.local.user.data.path";
    String LOCAL_PRODUCT_DATA_PATH = "spark.local.product.data.path";

    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    //"???
    String PARAM_PROFESSIONALS = "startAge";
    String PARAM_CITYS = "startAge";
    String PARAM_SEX = "startAge";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_ID = "categoryIDs";


}
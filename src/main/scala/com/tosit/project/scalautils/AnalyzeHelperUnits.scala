package com.tosit.project.scalautils

import com.tosit.project.constants.Constants
import com.tosit.project.javautils.ParamUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.json.JSONObject

/**
  * 辅助数据分析工具对象
  *
  * Created by Wanghan on 2017/3/11.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object AnalyzeHelperUnits {
    /**
      * 按照访问日期筛选用户访问行为
      *
      * @param sqlContext
      * @param json
      * @return
      */
    def getActionRddByDateRange(sqlContext: SQLContext, json: JSONObject): RDD[Row] = {
        val startDate = ParamUtils.getSingleValue(json, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getSingleValue(json, Constants.PARAM_END_DATE)
        val table = Constants.TABLE_USER_VISIT_ACTION
        val sql = "SELECT * FROM " + table + " WHERE date >= \"" + startDate + "\" AND date <= \"" + endDate + "\""
        sqlContext.sql(sql).rdd
    }

    /**
      * 按照用户ID获取用户信息
      *
      * @param sQLContext
      * @param user_id
      * @return
      */
    def getUserInfoWithUserID(sQLContext: SQLContext, user_id: Long): RDD[Row] = {
        val table = Constants.TABLE_USER_INFO
        val sql = "SELECT * FROM " + table + " WHERE user_id = " + user_id
        sQLContext.sql(sql).rdd
    }

    /**
      * 获取所有用户信息
      *
      * @param sQLContext
      * @return 类型为(user_id,Row)
      */
    def getUserInfo(sQLContext: SQLContext): RDD[(Long, Row)] = {
        val table = Constants.TABLE_USER_INFO
        val sql = "SELECT * FROM " + table
        sQLContext.sql(sql).rdd.map(s => (s.getLong(0), s))
    }

    /**
      * 根据商品ID返回商品信息
      *
      * @param sQLContext
      * @param category_id
      * @return
      */
    def getProductByCategoryId(sQLContext: SQLContext, category_id: String): RDD[Row] = {
        val table = Constants.TABLE_PRODUCT_INFO
        val sql = "SELECT * FROM " + table + " WHERE product_id = " + category_id
        sQLContext.sql(sql).rdd
    }
}

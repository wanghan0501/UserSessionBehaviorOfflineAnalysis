package com.tosit.project.scalautils

import com.tosit.project.conf.ConfigurationManager
import com.tosit.project.constants.Constants
import com.tosit.project.javautils.ParamUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.LongType
import org.json.JSONObject

/**
  * Created by Wanghan on 2017/3/11.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object AnalyzeUnits {
    /**
      * 加载SQL上下环境
      *
      * @param sc
      * @return
      */
    def getSQLContext(sc: SparkContext): SQLContext = {
        val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
        if (local) new SQLContext(sc) else new HiveContext(sc)
    }

    /**
      * 按照访问日期筛选用户访问行为
      *
      * @param sqlContext
      * @param json
      * @return
      */
    def getActionRddByDateRange(sqlContext: SQLContext, json: JSONObject): RDD[Row] = {
        val startDate = ParamUtils.getParam(json, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getParam(json, Constants.PARAM_END_DATE)
        val table = Constants.TABLE_USER_VISIT_ACTION
        val sql = "select * from " + table + " where date >= \"" + startDate + "\" and date <= \"" + endDate + "\""
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
        val sql = "select * from " + table + " where user_id = " + user_id + " "
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
        val sql = "select * from " + table
        sQLContext.sql(sql).rdd.map(s => (s.getLong(0), s))
    }
}

package com.tosit.project.scalautils

import com.tosit.project.constants.Constants
import com.tosit.project.javautils.{ParamUtils, SqlUnits}
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
      * 根据用户需求，编辑sql查询语句
      *
      * @param json
      * @return
      */
    def getSQL(json: JSONObject): (String, String) = {
        // 解析json值，获得用户的查询参数
        val startAge = ParamUtils.getSingleValue(json, Constants.PARAM_START_AGE)
        val endAge = ParamUtils.getSingleValue(json, Constants.PARAM_END_AGE)
        val startDate = ParamUtils.getSingleValue(json, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getSingleValue(json, Constants.PARAM_END_DATE)
        val professionals = ParamUtils.getMultipleValues(json, Constants.PARAM_PROFESSIONALS)
        val citys = ParamUtils.getMultipleValues(json, Constants.PARAM_CITYS)
        val sex = ParamUtils.getMultipleValues(json, Constants.PARAM_SEX)
        val searchWords = ParamUtils.getMultipleValues(json, Constants.PARAM_SEARCH_WORDS)
        val categoryIds = ParamUtils.getMultipleValues(json, Constants.PARAM_CATEGORY_IDS)

        // 准备sql查询user_Info表语句
        var sqlUserInfo: String = "SELECT * FROM " + Constants.TABLE_USER_INFO
        // 如果有起始年龄限定
        if (startAge != null) {
            val currentSql = " age >= " + startAge
            sqlUserInfo = SqlUnits.concatSQL(sqlUserInfo, currentSql)
        }
        // 如果有终止年龄限定
        if (endAge != null) {
            val currentSql = " age <= " + endAge
            sqlUserInfo = SqlUnits.concatSQL(sqlUserInfo, currentSql)
        }
        // 如果有职业限定
        if (professionals != null) {
            val iterator = professionals.iterator
            var currentSql: String = ""
            while (iterator.hasNext) {
                val currentProfessional = iterator.next()
                currentSql += (" professional = \"" + currentProfessional + "\" OR")
            }
            currentSql = SqlUnits.trimOr(currentSql)
            sqlUserInfo = SqlUnits.concatSQL(sqlUserInfo, currentSql)
        }

        // 如果有城市限定
        if (citys != null) {
            val iterator = citys.iterator
            var currentSql: String = ""
            while (iterator.hasNext) {
                val currentCity = iterator.next()
                currentSql += (" city = \"" + currentCity + "\" OR")
            }
            currentSql = SqlUnits.trimOr(currentSql)
            sqlUserInfo = SqlUnits.concatSQL(sqlUserInfo, currentSql)
        }

        // 如果有性别限定
        if (sex != null) {
            val iterator = citys.iterator
            var currentSql: String = ""
            while (iterator.hasNext) {
                val currentSex = iterator.next()
                currentSql += (" sex = \"" + currentSex + "\" OR")
            }
            currentSql = SqlUnits.trimOr(currentSql)
            sqlUserInfo = SqlUnits.concatSQL(sqlUserInfo, currentSql)
        }

        // 准备sql查询user_visit_action表语句
        var sqlUserVisitAction: String = "SELECT * FROM " + Constants.TABLE_USER_VISIT_ACTION
        // 如果有起始日期限定
        if (startDate != null) {
            val currentSql = " date >= \"" + startDate + "\""
            sqlUserVisitAction = SqlUnits.concatSQL(sqlUserVisitAction, currentSql)
        }

        // 如果有终止日期限定
        if (endDate != null) {
            val currentSql = " date <= \"" + endDate + "\""
            sqlUserVisitAction = SqlUnits.concatSQL(sqlUserVisitAction, currentSql)
        }
        // 如果有关键字限定
        if (searchWords != null) {
            val iterator = searchWords.iterator
            var currentSql: String = ""
            while (iterator.hasNext) {
                val currentSearchWord = iterator.next()
                currentSql += (" search_keyword = \"" + currentSearchWord + "\" OR")
            }
            currentSql = SqlUnits.trimOr(currentSql)
            sqlUserVisitAction = SqlUnits.concatSQL(sqlUserVisitAction, currentSql)
        }

        // 如果有点击品类限定
        if (categoryIds != null) {
            val iterator = categoryIds.iterator
            var currentSql: String = ""
            while (iterator.hasNext) {
                val currentCategoryId = iterator.next()
                currentSql += (" click_category_id = \"" + currentCategoryId + "\" OR")
            }
            currentSql = SqlUnits.trimOr(currentSql)
            sqlUserVisitAction = SqlUnits.concatSQL(sqlUserVisitAction, currentSql)
        }
//                //构造with···as···sql语句，效率较慢
//                if (searchWords != null || categoryIds != null)
//                    sqlUserVisitAction = SqlUnits.concatSQL(sqlUserVisitAction, Constants.TABLE_USER_VISIT_ACTION, "session_id")

        (sqlUserInfo, sqlUserVisitAction)
    }

    def getFullSession(sqlContext: SQLContext): RDD[Row] = {
        val table = Constants.TABLE_USER_VISIT_ACTION
        val sql = "SELECT * FROM " + table
        sqlContext.sql(sql).rdd
    }
}

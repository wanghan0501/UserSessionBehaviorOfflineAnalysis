package com.tosit.project.session

import com.tosit.project.constants.Constants
import com.tosit.project.dao.factory.DAOFActory
import com.tosit.project.javautils.{ParamUtils, StringUtils}
import com.tosit.project.scalautils.{AnalyzeHelperUnits, InitUnits, SparkUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.json.JSONObject


/**
  * 用户访问分析类
  * <p>
  * Created by Wanghan on 2017/3/11.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object UserVisitAnalyzeService {
    def main(args: Array[String]): Unit = {
        // 初始化spark环境
        val context = InitUnits.initSparkContext()
        val sc = context._1
        val sQLContext = context._2
        // 加载本地session访问日志测试数据
        SparkUtils.loadLocalTestDataToTmpTable(sc, sQLContext)
        // 创建DAO组件,DAO组件是用来操作数据库的
        //val taskDao = DAOFActory.getTaskDAO()
        // 通过任务常量名来获取任务ID
        //        val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_SESSION_TASKID)
        //        val task = if (taskId > 0) taskDao.findById(taskId) else null
        //        // 抛出task异常
        //        if (task == null) {
        //            throw new TaskException("Can't find task by id: " + taskId);
        //        }
        //        // 获取任务参数
        //        val taskParam = new JSONObject(task.getTaskParam)
        //
        //        val actionRdd = getActionRddByDateRange(sqlContext, taskParam)

        val param = new JSONObject("{\"startDate\":[\"2017-03-06\"],\"endDate\":[\"2017-03-06\"]}")
        val actionRddByDateRange = AnalyzeHelperUnits.getActionRddByDateRange(sQLContext, param)
        val res = aggregateBySession(sQLContext, actionRddByDateRange).collect().toBuffer
        print(res)
        sc.stop()
    }


    /**
      * 按照session聚合,返回值形如(sessionid,sessionid=value|searchword=value|clickcaterory=value|
      * age=value|professional=value|city=value|sex=value)
      *
      * @param sQLContext
      * @param actionRddByDateRange
      * @return
      */
    def aggregateBySession(sQLContext: SQLContext, actionRddByDateRange: RDD[Row]) = {
        // sessionidRddWithAction 形为(session_id,RDD[Row])
        val sessionIdRddWithAction = actionRddByDateRange.map(tuple => (tuple.getString(2), tuple)).groupByKey()

        // userIdRddWithSearchWordsAndClickCategoryIds 形为(user_id,session_id|searchWords|clickCategoryIds)
        val userIdRddWithSearchWordsAndClickCategoryIds = sessionIdRddWithAction.map(s => {
            val session_id: String = s._1
            // 用户ID
            var user_id: Long = 0L
            // 搜索关键字的集合
            var searchWords: String = ""
            // 点击分类ID的集合
            var clickCategoryIds: String = ""

            val iterator = s._2.iterator
            while (iterator.hasNext) {
                val row = iterator.next()
                user_id = row.getLong(1)
                val searchWord = row.getString(6).trim
                val clickCategoryId = row.getString(7).trim
                if (searchWord != "null" && !searchWords.contains(searchWord)) {
                    searchWords += (searchWord + ",")
                }
                if (clickCategoryId != "null" && !clickCategoryIds.contains(clickCategoryId)) {
                    clickCategoryIds += (clickCategoryId + ",")
                }
            }

            searchWords = StringUtils.trimComma(searchWords)
            clickCategoryIds = StringUtils.trimComma(clickCategoryIds)
            val userAggregateInfo = Constants.FIELD_SESSION_ID + "=" + session_id + Constants.VALUE_SEPARATOR +
                Constants.FIELD_SEARCH_KEYWORDS + "=" + searchWords + Constants.VALUE_SEPARATOR +
                Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds
            (user_id, userAggregateInfo)
        })

        // userInfo形如(user_id,RDD[Row])
        val userInfo = AnalyzeHelperUnits.getUserInfo(sQLContext)
        val userWithSessionInfoRdd = userInfo.join(userIdRddWithSearchWordsAndClickCategoryIds)

        userWithSessionInfoRdd.map(t => {
            val userAggregateInfo = t._2._2
            val userInfo = t._2._1
            val session_id = StringUtils.getFieldFromConcatString(userAggregateInfo, Constants.REGULAR_VALUE_SEPARATOR,
                Constants.FIELD_SESSION_ID)
            val age = userInfo.getInt(3)
            val professional = userInfo.getString(4)
            val city = userInfo.getString(5)
            val sex = userInfo.getString(6)

            // 形如(sessionid,sessionid=value|searchword=value|clickcaterory=value|age=value|professional=value|city=value|sex=value)
            val aggregateInfo = userAggregateInfo + Constants.VALUE_SEPARATOR +
                Constants.FIELD_AGE + "=" + age + Constants.VALUE_SEPARATOR +
                Constants.FIELD_PROFESSIONAL + "=" + professional + Constants.VALUE_SEPARATOR +
                Constants.FIELD_CITY + "=" + city + Constants.VALUE_SEPARATOR +
                Constants.FIELD_SEX + "=" + sex
            (session_id, aggregateInfo)
        })
    }

//    def aggregateByRequirement(sQLContext: SQLContext, json: JSONObject): RDD[(String, String)] = {
//        // 解析json值，获得用户的查询参数
//        val startAge = ParamUtils.getSingleValue(json, Constants.PARAM_START_AGE)
//        val endAge = ParamUtils.getSingleValue(json, Constants.PARAM_END_AGE)
//        val professionals = ParamUtils.getMultipleValues(json, Constants.PARAM_PROFESSIONALS)
//        val citys = ParamUtils.getMultipleValues(json, Constants.PARAM_CITYS)
//        val sex = ParamUtils.getMultipleValues(json, Constants.PARAM_SEX)
//        val searchWords = ParamUtils.getMultipleValues(json, Constants.PARAM_SEARCH_WORDS)
//        val categoryIds = ParamUtils.getMultipleValues(json, Constants.PARAM_CATEGORY_IDS)
//
//        // 准备sql查询语句
//        var sql: String = ""
//        if (startAge != null) sql += (" age >= " + startAge + " AND")
//        if (endAge != null) sql += (" age <= " + endAge + " AND")
//        if (professionals != null) {
//            val iterator = professionals.iterator
//            while (iterator.hasNext) {
//                val currentProfessional = iterator.next()
//                sql += (" professional = " + currentProfessional + " OR")
//            }
//        }
//        if (citys != null) {
//            val iterator = citys.iterator
//            while (iterator.hasNext) {
//                val currentCity = iterator.next()
//                sql += (" city = " + currentCity + " OR")
//            }
//        }
//        if (sex != null) {
//            val iterator = citys.iterator
//            while (iterator.hasNext) {
//                val currentSex = iterator.next()
//                sql += (" sex = " + currentSex + " OR")
//            }
//        }
//    }
}

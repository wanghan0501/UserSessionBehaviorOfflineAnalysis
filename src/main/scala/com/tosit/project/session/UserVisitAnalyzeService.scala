package com.tosit.project.session

import java.text.SimpleDateFormat
import java.util.{Date, NoSuchElementException}

import com.tosit.project.constants.Constants
import com.tosit.project.dao.factory.DAOFActory
import com.tosit.project.javautils.{ParamUtils, SqlUnits, StringUtils}
import com.tosit.project.scalautils.{AnalyzeHelperUnits, InitUnits, SparkUtils}
import org.apache.spark.SparkContext
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

        val param1 = new JSONObject("{\"startDate\":[\"2017-03-06\"],\"endDate\":[\"2017-03-06\"],\"startAge\":[\"40\"],\"endAge\":[\"42\"],\"citys\":[\"city14\"]}")
        println(param1.toString)

        //第二问
        //        val aggUserVisitAction = sQLContext.sql("SELECT * FROM user_visit_action WHERE ( date >= \"2017-03-06\") AND ( date <= \"2017-03-06\")").rdd
        //        val aggUserInfo = sQLContext.sql("SELECT * FROM user_info").rdd
        //        val res = aggregateBySession(sQLContext, aggUserInfo, aggUserVisitAction)
        //        print(res.collect().toBuffer)

        //        //第三问
        //        aggregateByRequirement(sQLContext, param1)
        //        val actionRddByDateRange = aggregateByRequirement(sQLContext, param1).collect().toBuffer
        //        println(actionRddByDateRange)

        //        //第四问
        //        val res = getVisitLengthAndStepLength(sc, sQLContext, param1)
        //        println(res)

        //        // 第五问
        //        val session = getSessionByRequirement(sQLContext, param1)
        //        val hotProducts = getFireProduct(session).iterator
        //        for (i <- hotProducts)
        //            print(i)

        sc.stop()
    }

    /**
      * 按照session聚合,返回值形如(sessionid,sessionid=value|searchword=value|clickcaterory=value|
      * age=value|professional=value|city=value|sex=value)
      *
      * @param sQLContext
      * @param aggUserInfo
      * @param aggUserVisitAction
      * @return
      */
    def aggregateBySession(sQLContext: SQLContext, aggUserInfo: RDD[Row], aggUserVisitAction: RDD[Row]): RDD[(String, String)] = {
        // sessionidRddWithAction 形为(session_id,RDD[Row])
        val sessionIdRddWithAction = aggUserVisitAction.map(tuple => (tuple.getString(2), tuple)).groupByKey()
        // userIdRddWithSearchWordsAndClickCategoryIds 形为(user_id,session_id|searchWords|clickCategoryIds)
        val userIdRddWithSearchWordsAndClickCategoryIds = sessionIdRddWithAction.map(f = s => {
            val session_id: String = s._1
            // 用户ID
            var user_id: Long = 0L
            // 搜索关键字的集合
            var searchWords: String = ""
            // 点击分类ID的集合
            var clickCategoryIds: String = ""
            //session的起始时间
            var startTime: Date = null
            // session的终止时间
            var endTime: Date = null
            // 访问步长
            var stepLength = 0

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
                //  步长更新
                stepLength += 1

                val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val actionTime = TIME_FORMAT.parse(row.getString(4) + " " + row.getString(5))

                if (startTime == null && endTime == null) {
                    startTime = actionTime
                    endTime = actionTime
                } else if (actionTime.before(startTime)) {
                    startTime = actionTime
                } else if (actionTime.after(endTime)) {
                    endTime = actionTime
                }
            }
            // 访问时常
            val visitLength = (endTime.getTime - startTime.getTime) / 1000
            //val visitLength = 0
            searchWords = StringUtils.trimComma(searchWords)
            clickCategoryIds = StringUtils.trimComma(clickCategoryIds)
            val userAggregateInfo = Constants.FIELD_SESSION_ID + "=" + session_id + Constants.VALUE_SEPARATOR +
                Constants.FIELD_SEARCH_KEYWORDS + "=" + searchWords + Constants.VALUE_SEPARATOR +
                Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + Constants.VALUE_SEPARATOR +
                Constants.FIELD_VISIT_LENGTH + "=" + visitLength + Constants.VALUE_SEPARATOR +
                Constants.FIELD_STEP_LENGTH + "=" + stepLength
            (user_id, userAggregateInfo)
        })

        // userInfo形如(user_id,RDD[Row])
        val userInfo = aggUserInfo.map(tuple => (tuple.getLong(0), tuple))

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

    /**
      * 根据传入json参数进行类聚
      *
      * @param sQLContext
      * @param json
      * @return
      */
    def aggregateByRequirement(sQLContext: SQLContext, json: JSONObject): RDD[(String, String)] = {
        val sql = AnalyzeHelperUnits.getSQL(sQLContext, json)
        val sqlUserInfo = sql._1
        val sqlUserVisitAction = sql._2
        val aggUserInfo = sQLContext.sql(sqlUserInfo).rdd
        val aggUserVisitAction = sQLContext.sql(sqlUserVisitAction).rdd
        aggregateBySession(sQLContext, aggUserInfo, aggUserVisitAction)
    }

    /**
      * 实现自定义累加器完成多个聚合统计业务的计算，
      * 统计业务包括访问时长：1~3秒，4~6秒，7~9秒，10~30秒，30~60秒的session访问量统计，
      * 访问步长：1~3个页面，4~6个页面等步长的访问统计
      *
      * @param sc
      * @param sQLContext
      * @param json
      * @return 形如(session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0)
      */
    def getVisitLengthAndStepLength(sc: SparkContext, sQLContext: SQLContext, json: JSONObject): String = {

        val sessionAggAccumulator = sc.accumulator("")(SessionAggAccumulator)
        // 获取session聚合信息
        val rdd = aggregateByRequirement(sQLContext, json)
        rdd.foreach(tuple => {
            val currentInfo = tuple._2
            // 获取访问时长
            val visitLength = StringUtils.getFieldFromConcatString(currentInfo, Constants.REGULAR_VALUE_SEPARATOR, Constants.FIELD_VISIT_LENGTH).toLong
            // 获取访问步长
            val stepLength = StringUtils.getFieldFromConcatString(currentInfo, Constants.REGULAR_VALUE_SEPARATOR, Constants.FIELD_STEP_LENGTH).toLong

            sessionAggAccumulator.add(Constants.SESSION_COUNT)
            // 根据访问时长，更新累加器
            if (visitLength >= 1 && visitLength <= 3)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            else if (visitLength >= 4 && visitLength <= 6)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            else if (visitLength >= 7 && visitLength <= 9)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            else if (visitLength >= 10 && visitLength <= 30)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            else if (visitLength >= 31 && visitLength <= 60)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            else if (visitLength >= 61 && visitLength <= 180)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            else if (visitLength >= 181 && visitLength <= 600)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            else if (visitLength >= 601 && visitLength <= 1800)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            else if (visitLength >= 1801)
                sessionAggAccumulator.add(Constants.TIME_PERIOD_30m)

            // 根据访问步长，更新累加器
            if (stepLength >= 1 && stepLength <= 3)
                sessionAggAccumulator.add(Constants.STEP_PERIOD_1_3)
            else if (stepLength >= 4 && stepLength >= 6)
                sessionAggAccumulator.add(Constants.STEP_PERIOD_4_6)
            else if (stepLength >= 7 && stepLength <= 9)
                sessionAggAccumulator.add(Constants.STEP_PERIOD_7_9)
            else if (stepLength >= 10 && stepLength <= 30)
                sessionAggAccumulator.add(Constants.STEP_PERIOD_10_30)
            else if (stepLength >= 31 && stepLength <= 60)
                sessionAggAccumulator.add(Constants.STEP_PERIOD_30_60)
            else if (stepLength >= 61)
                sessionAggAccumulator.add(Constants.STEP_PERIOD_60)

        })
        sessionAggAccumulator.value
    }

    /**
      * 根据用户需求获取筛选后的session
      *
      * @param sQLContext
      * @param jSONObject
      * @return
      */
    def getSessionByRequirement(sQLContext: SQLContext, jSONObject: JSONObject): RDD[Row] = {
        val sql = AnalyzeHelperUnits.getSQL(sQLContext, jSONObject)
        val sqlUserInfo = sql._1
        val sqlUserVisitAction = sql._2
        // 形如(uesr_id,RDD)
        val aggUserInfo = sQLContext.sql(sqlUserInfo).rdd.map(t => (t.getLong(0), t))
        // 形如(uesr_id,RDD)
        val aggUserVisitAction = sQLContext.sql(sqlUserVisitAction).rdd.map(t => (t.getLong(1), t))
        aggUserInfo.join(aggUserVisitAction).map(t => {
            t._2._2
        })
    }


    /**
      * 对通过筛选条件的session，按照各个品类的点击、下单和支付次数，降序排列，获取前10个热门品类。优先级：点击，下单，支付。
      *
      * @param rDD
      * @return
      */
    def getFireProduct(rDD: RDD[Row]): Array[(String, Int, Int, Int)] = {

        def getCatogeryIdAndTimes(index: Int, rDD: RDD[Row]): RDD[(String, Int)] = {
            // 类聚
            val aggByIndex = rDD.groupBy(x => x.get(index))
            // 统计每一品类的数量
            val count = aggByIndex.filter(x => !x._1.equals("null")).map(x => (x._1.toString, x._2.size))
            count
        }

        val rdd_click = getCatogeryIdAndTimes(7, rDD)
        val rdd_order = getCatogeryIdAndTimes(9, rDD)
        val rdd_pay = getCatogeryIdAndTimes(11, rDD)

        val fullRdd = rdd_click.fullOuterJoin(rdd_order).fullOuterJoin(rdd_pay)

        val sortedFullRdd = fullRdd.map(tuple => {
            val categoryId: String = tuple._1
            val click: Int = if (tuple._2._1.isEmpty) 0 else if (tuple._2._1.get._1.isEmpty) 0 else tuple._2._1.get._1.get
            val order: Int = if (tuple._2._1.isEmpty) 0 else if (tuple._2._1.get._2.isEmpty) 0 else tuple._2._1.get._2.get
            val pay: Int = if (tuple._2._2.isEmpty) 0 else tuple._2._2.get

            (new SessionPair(categoryId, click, order, pay), tuple)
        }).sortByKey(false)

        sortedFullRdd.map(tuple => (tuple._1.categoryId, tuple._1.click, tuple._1.order, tuple._1.pay)).take(10)
    }


}

package com.tosit.project.session

import java.text.SimpleDateFormat
import java.util.{Date}

import com.tosit.project.constants.Constants
import com.tosit.project.dao.factory.DAOFActory
import com.tosit.project.exception.TaskException
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
        //        // 创建DAO组件,DAO组件是用来操作数据库的
        //        val taskDao = DAOFActory.getTaskDAO()
        //        // 通过任务常量名来获取任务ID
        //        val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_SESSION_TASKID)
        //        val task = if (taskId > 0) taskDao.findById(taskId) else null
        //        // 抛出task异常
        //        if (task == null) {
        //            throw new TaskException("Can't find task by id: " + taskId);
        //        }
        //        // 获取任务参数
        //        val taskParam = new JSONObject(task.getTaskParam)

        val param1 = new JSONObject("{\"startDate\":[\"2017-03-06\"],\"endDate\":[\"2017-03-06\"],\"startAge\":[\"40\"],\"endAge\":[\"42\"],\"citys\":[\"city14\"],\"searchWords\":[\"小米5\"]}")
        println(param1.toString)

        //        //第二问
        //        val aggUserVisitAction = sQLContext.sql("SELECT * FROM user_visit_action WHERE ( date >= \"2017-03-06\") AND ( date <= \"2017-03-06\")").rdd
        //        val aggUserInfo = sQLContext.sql("SELECT * FROM user_info").rdd
        //        val res = displaySession(aggUserInfo, aggUserVisitAction)
        //        print(res.collect().toBuffer)

        //        //第三问
        //        val actionRddByDateRange = sessionAggregateByRequirement(sQLContext, param1).collect().toBuffer
        //        println(actionRddByDateRange)

        //        //第四问
        //        val res = getVisitLengthAndStepLength(sc, sQLContext, param1)
        //        println(res)
        //
        //        // 第五问
        //        val session = getSessionByRequirement(sQLContext, param1)
        //        val hotProducts = getHotCategory(session).iterator
        //        for (i <- hotProducts)
        //            print(i)

        sc.stop()
    }

    /**
      * 将输入的userInfo和userVisitAction按照指定形式展示出来,返回值形如(sessionid,
      * sessionid=value|searchword=value|clickcaterory=value|age=value|professional=value|city=value|sex=value)
      *
      * @param aggUserInfo
      * @param aggUserVisitAction
      * @return
      */
    def displaySession(aggUserInfo: RDD[Row], aggUserVisitAction: RDD[Row]): RDD[(String, String)] = {
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

            // 形如(sessionid,sessionid=value|searchword=value|clickcategory=value|age=value|professional=value|city=value|sex=value)
            val aggregateInfo = userAggregateInfo + Constants.VALUE_SEPARATOR +
                Constants.FIELD_AGE + "=" + age + Constants.VALUE_SEPARATOR +
                Constants.FIELD_PROFESSIONAL + "=" + professional + Constants.VALUE_SEPARATOR +
                Constants.FIELD_CITY + "=" + city + Constants.VALUE_SEPARATOR +
                Constants.FIELD_SEX + "=" + sex
            (session_id, aggregateInfo)
        })
    }


    /**
      * session根据传入json参数进行类聚,输出类型如(sessionid,sessionid=value|searchword=value|clickcaterory=value|
      * age=value|professional=value|city=value|sex=value)
      *
      * @param sQLContext
      * @param json
      * @return
      */
    def sessionAggregateByRequirement(sQLContext: SQLContext, json: JSONObject): RDD[(String, String)] = {
        val sql = AnalyzeHelperUnits.getSQL(json)
        val sqlUserInfo = sql._1
        val sqlUserVisitAction = sql._2
        val aggUserInfo = sQLContext.sql(sqlUserInfo).rdd
        // 形如(sessionId,Row)
        val partialVisitAction = sQLContext.sql(sqlUserVisitAction).rdd.map(t => (t.getString(2), t))
        val fullVisitAction = AnalyzeHelperUnits.getFullSession(sQLContext).map(t => (t.getString(2), t))
        val aggUserVisitAction = partialVisitAction.join(fullVisitAction).map(t => t._2._2)
        displaySession(aggUserInfo, aggUserVisitAction)
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
        val rdd = sessionAggregateByRequirement(sQLContext, json)
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
        val sql = AnalyzeHelperUnits.getSQL(jSONObject)
        val sqlUserInfo = sql._1
        val sqlUserVisitAction = sql._2
        // 形如(user_id,RDD)
        val aggUserInfo = sQLContext.sql(sqlUserInfo).rdd.map(t => (t.getLong(0), t))
        // 形如(sessionId,Row)
        val partialVisitAction = sQLContext.sql(sqlUserVisitAction).rdd.map(t => (t.getString(2), t))
        // 形如(sessionId,Row)
        val fullVisitAction = AnalyzeHelperUnits.getFullSession(sQLContext).map(t => (t.getString(2), t))
        // 形如(user_id,RDD)
        val aggUserVisitAction = partialVisitAction.join(fullVisitAction).map(t => (t._2._2.getLong(1), t._2._2))
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
    def getHotCategory(rDD: RDD[Row]): Array[(String, Int, Int, Int)] = {

        /**
          * category排序构造方法
          *
          * @param category_id
          * @param click_times
          * @param order_times
          * @param pay_times
          */
        class SessionPair(category_id: String, click_times: Int, order_times: Int, pay_times: Int) extends Ordered[SessionPair] with Serializable {
            val categoryId = category_id
            val click = click_times
            val order = order_times
            val pay = pay_times

            /**
              * 重载比较方法
              *
              * @param that
              * @return
              */
            override def compare(that: SessionPair): Int = {
                if (this.click == that.click) {
                    if (this.order == that.order) {
                        return this.click - that.click
                    }
                    return this.order - that.order
                }
                return this.click - that.click
            }
        }

        /**
          * 按照品类聚合并得到每类次数
          *
          * @param index
          * @param rDD
          * @return
          */
        def getCategoryIdAndTimes(index: Int, rDD: RDD[Row]): RDD[(String, Int)] = {
            // 类聚
            val aggByIndex = rDD.groupBy(x => x.get(index))
            // 统计每一品类的数量
            val count = aggByIndex.filter(x => !x._1.equals("null")).map(x => (x._1.toString, x._2.size))
            count
        }

        // 点击品类类聚
        val rdd_click = getCategoryIdAndTimes(7, rDD)
        // 下单品类类聚
        val rdd_order = getCategoryIdAndTimes(9, rDD)
        // 支付品类类聚
        val rdd_pay = getCategoryIdAndTimes(11, rDD)
        // 品类全连接
        val fullRdd = rdd_click.fullOuterJoin(rdd_order).fullOuterJoin(rdd_pay)
        // 利用SessionPair类排序
        val sortedFullRdd = fullRdd.map(tuple => {
            val categoryId: String = tuple._1
            val click: Int = if (tuple._2._1.isEmpty) 0 else if (tuple._2._1.get._1.isEmpty) 0 else tuple._2._1.get._1.get
            val order: Int = if (tuple._2._1.isEmpty) 0 else if (tuple._2._1.get._2.isEmpty) 0 else tuple._2._1.get._2.get
            val pay: Int = if (tuple._2._2.isEmpty) 0 else tuple._2._2.get

            (new SessionPair(categoryId, click, order, pay), tuple)
        }).sortByKey(false)
        // 输出top品类
        sortedFullRdd.map(tuple => (tuple._1.categoryId, tuple._1.click, tuple._1.order, tuple._1.pay)).take(10)
    }


}

package com.tosit.project.session

import com.tosit.project.conf.ConfigurationManager
import com.tosit.project.constants.Constants
import com.tosit.project.dao.factory.DAOFActory
import com.tosit.project.exception.TaskException
import com.tosit.project.javautils.ParamUtils
import com.tosit.project.scalautils.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject


/**
  * 用户访问分析类
  * <p>
  * Created by Wanghan on 2017/3/11.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object UserVisitAnalyzeService {
    def main(args: Array[String]): Unit = {
        // spark配置文件
        val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local[2]")
        // spark上下文环境
        val sc = new SparkContext(conf)
        // SQL上下文环境
        val sqlContext = getSQLContext(sc)
        // 加载本地session访问日志测试数据
        SparkUtils.loadLocalTestDataToTmpTable(sc, sqlContext)
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
        sc.stop()
    }

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
        val sql = "select* from " + table + " where date >= \"" + startDate + "\" and date <= \"" + endDate + "\""
        sqlContext.sql(sql).rdd
    }

    /**
      * 按照用户年龄段筛选用户
      *
      * @param sQLContext
      * @param json
      * @return
      */
    def getActionRddByAgeRange(sQLContext: SQLContext, json: JSONObject): RDD[Row] = {
        val startAge = ParamUtils.getParam(json, Constants.PARAM_START_AGE)
        val endAge = ParamUtils.getParam(json, Constants.PARAM_END_AGE)
        val table = Constants.TABLE_USER_INFO
        val sql = "select* from " + table + " where age >= \"" + startAge + "\" and age <= \"" + endAge + "\""
        sQLContext.sql(sql).rdd
    }

    //    def aggregateBySession(sQLContext: SQLContext, actionRddByDateRange: RDD[Row]): RDD[(String, String)] = {
    //        val actionRdd = actionRddByDateRange.map(t => (t.getString(2), t)).groupByKey()
    //
    //    }
}

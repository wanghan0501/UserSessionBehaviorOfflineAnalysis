package com.tosit.project.scalautils
import com.tosit.project.conf.ConfigurationManager
import com.tosit.project.constants.Constants
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._



object SparkUtils {

    /**
      * 加载本地测试数据到注册表
      * @param sc
      * @param sqlContext
      */
    def loadLocalTestDataToTmpTable(sc: SparkContext,sqlContext: SQLContext): Unit ={

        //读入用户session日志数据并注册为临时表
        //首先指定临时表的Schema
        val sessionschema = StructType(
            List(
                StructField("date",StringType,true),
                StructField("user_id",LongType,true),
                StructField("sessiopn_id",StringType,true),
                StructField("page_id",LongType,true),
                StructField("action_time",StringType,true),
                StructField("search_keyword",StringType,true),
                StructField("click_category_id",LongType,true),
                StructField("click_product_id",LongType,true),
                StructField("order_category_ids",StringType,true),
                StructField("order_product_ids",StringType,true),
                StructField("pay_category_ids",StringType,true),
                StructField("pay_product_ids",StringType,true),
                StructField("city_id",LongType,true)
            )
        )

        //从指定位置创建RDD
        val session_path = ConfigurationManager.getProperty(Constants.LOCAL_SESSION_DATA_PATH)
        val sessionRDD = sc.textFile(session_path).map(_.split(" "))
        //将RDD映射成rowRDD
        val sessionrowRDD = sessionRDD.map(s => Row(s(0).trim,s(1).toLong,s(2).trim,
            s(3).toLong, s(4).trim, s(5).trim, s(6).toLong,s(7).toLong,s(8).trim,
            s(9).trim, s(10).trim, s(11).trim, s(12).toLong))
        //import sqlContext.implicits._

        //将schema信息应用到rowRDD上
        val sessionDataFrame = sqlContext.createDataFrame(sessionrowRDD,sessionschema)

        //注册临时sessionAction表
        sessionDataFrame.registerTempTable(Constants.TABLE_USER_VISIT_ACTION)


        //定义用户数据schema
        val userschema = StructType(
            List(
                StructField("user_id",LongType,true),
                StructField("username",StringType,true),
                StructField("name",StringType,true),
                StructField("age",IntegerType,true),
                StructField("professional",StringType,true),
                StructField("city",StringType,true),
                StructField("sex",StringType,true)
            )
        )
        //从指定位置创建RDD
        val user_path = ConfigurationManager.getProperty(Constants.LOCAL_USER_DATA_PATH)
        val userRDD = sc.textFile(user_path).map(_.split(" "))
        val userrowRdd = userRDD.map(u => Row(u(0).toLong,u(1).trim,u(2).trim,u(3).toInt,u(4).trim,
            u(5).trim,u(6).trim))
        val userDataFrame = sqlContext.createDataFrame(userrowRdd,userschema)
        //注册用户信息临时表
        userDataFrame.registerTempTable(Constants.TABLE_USER_INFO)


        //定义商品数据schema
        val productchema = StructType(
            List(
                StructField("product_id",LongType,true),
                StructField("product_title",StringType,true),
                StructField("extend_info",StringType,true)
            )
        )
        //从指定位置创建RDD
        val product_path = ConfigurationManager.getProperty(Constants.LOCAL_PRODUCT_DATA_PATH)
        val productRDD = sc.textFile(product_path).map(_.split(" "))
        val productrowRdd = userRDD.map(u => Row(u(0).toLong,u(1).trim,u(2).trim))
        val productDataFrame = sqlContext.createDataFrame(productrowRdd,productchema)
        //注册用户信息临时表
        productDataFrame.registerTempTable(Constants.TABLE_PRODUCT_INFO)


    }

}
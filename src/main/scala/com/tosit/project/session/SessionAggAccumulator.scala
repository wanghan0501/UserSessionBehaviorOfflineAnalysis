package com.tosit.project.session

import com.tosit.project.constants.Constants
import com.tosit.project.exception.StringSepatorException
import com.tosit.project.javautils.StringUtils
import org.apache.spark.AccumulatorParam

/**
  * 自定义session聚合累加器
  *
  * Created by Wanghan on 2017/3/13.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object SessionAggAccumulator extends AccumulatorParam[String] {
    private val INITIAL_VALUE: String = Constants.SESSION_COUNT + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_1s_3s + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_4s_6s + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_7s_9s + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_10s_30s + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_30s_60s + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_1m_3m + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_3m_10m + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_10m_30m + "=0" + Constants.VALUE_SEPARATOR +
        Constants.TIME_PERIOD_30m + "=0" + Constants.VALUE_SEPARATOR +
        Constants.STEP_PERIOD_1_3 + "=0" + Constants.VALUE_SEPARATOR +
        Constants.STEP_PERIOD_4_6 + "=0" + Constants.VALUE_SEPARATOR +
        Constants.STEP_PERIOD_7_9 + "=0" + Constants.VALUE_SEPARATOR +
        Constants.STEP_PERIOD_10_30 + "=0" + Constants.VALUE_SEPARATOR +
        Constants.STEP_PERIOD_30_60 + "=0" + Constants.VALUE_SEPARATOR +
        Constants.STEP_PERIOD_60 + "=0"

    /**
      * zero方法，其实主要用于数据的初始化
      * 那么，我们这里，就返回一个值，就是初始化中，所有范围区间的数量，多少0,
      * 各个范围区间的统计数量的拼接，还是采用|分割。
      *
      * @param initialValue 初始化数据
      * @return
      */
    override def zero(initialValue: String): String = {
        INITIAL_VALUE
    }

    /**
      * 更新计数器，在r1中，找到r2对应的value，累加1，然后再更新回连接串里面去
      *
      * @param r1 初始化的那个连接串
      * @param r2 遍历session的时候，判断出某个session对应的区间,对应的连接串
      */
    override def addInPlace(r1: String, r2: String): String = {
        if (r1 == "")
            r2
        else {
            try {
                //累加1
                val oldValue = StringUtils.getFieldFromConcatString(r1, Constants.REGULAR_VALUE_SEPARATOR, r2)
                val newValue = oldValue.toInt + 1
                //更新连接串
                StringUtils.setFieldInConcatString(r1, Constants.REGULAR_VALUE_SEPARATOR, r2, newValue.toString)
            } catch {
                case e: Exception => r2
            }
        }
    }

}

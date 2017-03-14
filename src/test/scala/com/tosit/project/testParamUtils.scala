package com.tosit.project

import com.tosit.project.javautils.ParamUtils
import com.tosit.project.scalautils.AnalyzeHelperUnits
import org.json.JSONObject

/**
  * Created by Wanghan on 2017/3/12.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object testParamUtils {
    def main(args: Array[String]): Unit = {
        val param1 = new JSONObject("{\"startDate\":[\"2017-03-06\"],\"endDate\":[\"2017-03-06\"],\"startAge\":[\"40\"],\"endAge\":[\"42\"],\"citys\":[\"city14\"],\"searchWords\":[\"小米5\",\"小米1\"]}")
        println(param1.toString)
        val res = AnalyzeHelperUnits.getSQL(param1)
        val a1 = res._1
        val a2 = res._2
        println(a1)
        println(a2)


    }
}

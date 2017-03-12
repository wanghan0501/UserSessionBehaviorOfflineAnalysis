package com.tosit.project

import com.tosit.project.javautils.ParamUtils
import org.json.JSONObject

/**
  * Created by Wanghan on 2017/3/12.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object testParamUtils {
    def main(args: Array[String]): Unit = {
        val json = new JSONObject("{\"A\":[\"b\",\"c\"]}")
        println(json.toString)
        val res = ParamUtils.getMultipleValues(json, "A")
        val iterator = res.iterator
        while (iterator.hasNext) {
            println(iterator.next())
        }
    }
}

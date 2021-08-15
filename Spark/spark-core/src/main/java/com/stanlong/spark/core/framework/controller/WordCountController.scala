package com.stanlong.spark.core.framework.controller

import com.stanlong.spark.core.framework.common.TController
import com.stanlong.spark.core.framework.service.WordCountService

class WordCountController extends TController{

    private val wordCountService = new WordCountService

    def dispatch(): Unit ={
        val array = wordCountService.dataAnalysis()
        array.foreach(println)
    }

}

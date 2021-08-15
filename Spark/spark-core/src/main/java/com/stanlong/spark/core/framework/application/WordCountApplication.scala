package com.stanlong.spark.core.framework.application

import com.stanlong.spark.core.framework.common.TApplication
import com.stanlong.spark.core.framework.controller.WordCountController

object WordCountApplication extends App  with TApplication{

    start(){
        val controller = new WordCountController
        controller.dispatch()
    }


}

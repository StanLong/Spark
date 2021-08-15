package com.stanlong.spark.core.framework.common

import com.stanlong.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

    // 控制抽象
    def start(master:String = "local[*]", app:String="Application")(op : => Unit): Unit ={
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparkConf)

        // sc 优化， 存储到ThreadLocal里
        EnvUtil.put(sc)

        try{
            op
        }catch {
            case ex => println(ex.getMessage)
        }



        sc.stop()
        EnvUtil.clear()
    }
}

package com.stanlong.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))
        var user = new User()
        rdd.foreach( // rdd 中传递的函数是会包含闭包操作的，
            num =>{
                println("age=" + (user.age + num))
            }
        )
        sc.stop()
    }
}

class User {
    var age = 30
}
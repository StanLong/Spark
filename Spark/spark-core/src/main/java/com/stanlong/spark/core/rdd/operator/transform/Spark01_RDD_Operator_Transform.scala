package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        //
        val rdd = sc.makeRDD(List(List(1,2), 3,  List(4,5)) )
        val flatRdd = rdd.flatMap {
                    
                    case list: List[_] => list
                    case dat => List(dat)
                }
        flatRdd.collect().foreach(println)







        sc.stop()
    }
}


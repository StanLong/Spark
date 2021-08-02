package com.stanlong.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // aggregateByKey 有两个参数列表
        // 第一个参数列表 需要传递两个参数， 第一个参数表示初始值
        // 第二个参数列表 需要传递两个参数， 第一个表示分区内的计算规则，第二个表示分区间的计算规则
        // 最终的返回结果应该和初始值的类型保持一致
        // val rdd = sc.makeRDD(List(("a", 1),("a", 2),("a", 3),("a",4)), 2)
        // rdd.aggregateByKey(0 )(
        //     (x, y) => math.max(x,y),
        //     (x, y) => x+y
        // ).collect().foreach(println)

        // 获取相同key的数据的平均值
        val rdd = sc.makeRDD(List(("a", 1),("a", 2),("b", 3),("b",4),("b",5),("a",6)), 2)
        val newRdd = rdd.aggregateByKey((0,0))( // (0,0) 第一个0表示初始值，用于计算数据的总和，第二个0表示相同的key出现的次数
            (t, v) =>{
                (t._1 +v , t._2 + 1) // (数据相加，次数相加)
            }, // 分区内计算
            (t1, t2) =>{
                (t1._1 + t2._1, t1._2 + t2._2)
            } // 分区间计算
        )

        val resultRdd = newRdd.mapValues { // mapValues， key不变，只对value进行计算
            case (num, cnt) => {
                num / cnt
            }
        }

        resultRdd.collect().foreach(println)







        sc.stop()
    }
}


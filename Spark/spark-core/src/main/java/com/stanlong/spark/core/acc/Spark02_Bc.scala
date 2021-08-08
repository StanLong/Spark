package com.stanlong.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Bc{
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("ACC")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.makeRDD(List(("a", 1),("b", 2),("c", 3)))
        // val rdd2 = sc.makeRDD(List(("a", 4),("b", 5),("c", 6)))

        // join 会导致数据几何增长，并且会影响shuffle的性能，不推荐使用
        // val joinRdd = rdd1.join(rdd2)
        // joinRdd.collect().foreach(println)

        val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

        // join 的替代方法
        // rdd1.map{
        //     case(w, c) => {
        //         val l = map.getOrElse(w, 0)
        //         (w, (l,1))
        //     }
        // }.collect().foreach(println)

        // 使用广播变量
        val bc = sc.broadcast(map)
        rdd1.map{
            case(w,c) => {
                // 访问广播变量
                bc.value.getOrElse(w, 0)
                (w, (c,1))
            }
        }.collect().foreach(println)



        sc.stop()


    }
}

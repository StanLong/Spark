package com.stanlong.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // 1. 获取原始数据： 时间戳 省份 城市 用户 广告
        val dataRdd = sc.textFile("datas/agent.txt")

        // 2. 将原始数据进行结构转换
        // 时间戳 省份 城市 用户 广告 =》 ((省份, 广告), 1)
        val mapRdd = dataRdd.map(
            line => {
                val datas = line.split(" ")
                ((datas(1), datas(4)), 1)
            }
        )

        // 3. 将转换结构后的数据进行分组聚合
        // ((省份, 广告), 1) => ((省份, 广告), sum)
        val reduceRdd = mapRdd.reduceByKey(_ + _)

        // 4. 将聚合的结果进行结构转换
        // (省份,( 广告, sum))
        val newMapRdd = reduceRdd.map {
            case ((prv, ad), sum) => {
                (prv, (ad, sum))
            }
        }

        // 5. 将转换结构后的数据按照省份进行分组
        // (省份,[(广告A, sum), (广告B, sum)])
        val groupRdd = newMapRdd.groupByKey()

        // 6. 分组后的数据按照组内排序（降序）, 取前三
        val resultRdd = groupRdd.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )

        // 7.采集数据打印到控制台
        resultRdd.collect().foreach(println)



        sc.stop()
    }
}


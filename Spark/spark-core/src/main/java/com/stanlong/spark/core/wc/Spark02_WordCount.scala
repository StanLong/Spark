package com.stanlong.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {

    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        // 执行业务操作
        val lines = sc.textFile("datas/*") // 读取文件
        val words = lines.flatMap(_.split(" ")) // 获取一行一行的数据,扁平化操作：将数据按空格隔开
        val wordToOne = words.map(
            word => (word , 1)
        )
        val wordGroup = wordToOne.groupBy(
            t => t._1
        )

        val wordToCount = wordGroup.map{ // 对分组后的数据进行转换
            case(word, list) =>{
                list.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
            }
        }
        val array = wordToCount.collect()
        array.foreach(println) // 将转换结果采集到控制台打印出来

        // 关闭连接
        sc.stop()
    }
}

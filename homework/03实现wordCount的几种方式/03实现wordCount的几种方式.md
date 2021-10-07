```scala
package com.stanlong.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {

    def main(args: Array[String]): Unit = {
        // 建立和Spark框架的连接
        val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)

        // wordCount1(sc)
        // wordCount2(sc)
        // wordCount3(sc)
        // wordCount4(sc)
        // wordCount5(sc)
        // wordCount6(sc)
        // wordCount7(sc)
        // wordCount8(sc)
        wordCount91011(sc)

        // 关闭连接
        sc.stop()
    }

    // groupBy
    def wordCount1(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val groupRdd = words.groupBy(word => word)
        val wordCount = groupRdd.mapValues(iter => iter.size)
        wordCount.collect().foreach(println)
    }

    // groupByKey
    // groupByKey 有 shuffle 操作，效率不高
    def wordCount2(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val group = wordOne.groupByKey()
        val wordCount = group.mapValues(iter => iter.size)
        wordCount.collect().foreach(println)
    }

    // reduceByKey
    def wordCount3(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount = wordOne.reduceByKey(_+_)
        wordCount.collect().foreach(println)
    }

    // aggregateByKey
    def wordCount4(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"), 2)
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount = wordOne.aggregateByKey(0)(_+_, _+_)
        wordCount.collect().foreach(println)
    }

    // foldByKey
    def wordCount5(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount = wordOne.foldByKey(0)(_+_)
        wordCount.collect().foreach(println)
    }


    // combineByKey
    def wordCount6(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount = wordOne.combineByKey(
            v=>v,
            (x:Int, y) => x+y,
            (x:Int, y:Int) => x+y
        )
        wordCount.collect().foreach(println)
    }

    // countByKey
    def wordCount7(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_, 1))
        val wordCount = wordOne.countByKey()
        println(wordCount)
    }

    // countByValue
    def wordCount8(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordCount = words.countByValue()
        println(wordCount)
    }

    // reduce, aggregate, fold
    def wordCount91011(sc:SparkContext) :Unit={
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val mapWord = words.map(
            word => {
                mutable.Map[String, Long]((word, 1))
            }
        )
        val wordCount = mapWord.reduce(
            (map1, map2) => {
                map2.foreach{
                    case(word, count) =>{
                        val newCount = map1.getOrElse(word, 0L) + count
                        map1.update(word, newCount)
                    }
                }
                map1
            }
        )
        println(wordCount)
    }

}
```


# SparkStreaming

## 概述

SparkStreaming 准实时（以秒或者分钟为单位）， 微批次(时间)的数据处理框架。

Spark Streaming 用于流式数据的处理。Spark Streaming 支持的数据输入源很多，例如：Kafka、 Flume、Twitter、ZeroMQ 和简单的 TCP 套接字等等。数据输入后可以用 Spark 的高度抽象原语如：map、reduce、join、window 等进行运算。而结果也能保存在很多地方，如 HDFS，数据库等。

![](./doc/68.png)

和 Spark 基于 RDD 的概念很相似，Spark Streaming 使用离散化流(discretized stream)作为抽象表示，叫作DStream。DStream 是随时间推移而收到的数据的序列。在内部，每个时间区间收到的数据都作为 RDD 存在，而 DStream 是由这些RDD 所组成的序列(因此得名“离散化”)。所以简单来将，DStream 就是对 RDD 在实时数据处理场景的一种封装。

## 特点

- 易用
- 容错
- 易整合到Sparkt体系

## 架构图

- 整体架构图

  ![](./doc/69.png)

- SparkStreaming架构图

  ![](./doc/70.png)

## 背压机制

Spark 1.5 以前版本，用户如果要限制 Receiver 的数据接收速率，可以通过设置静态配制参

数“spark.streaming.receiver.maxRate”的值来实现，此举虽然可以通过限制接收速率，来适配当前的处理能力，

防止内存溢出，但也会引入其它问题。比如：producer 数据生产高于 maxRate，当前集群处理能力也高于 

maxRate，这就会造成资源利用率下降等问题。为了更好的协调数据接收速率与资源处理能力，1.5 版本开始 

Spark Streaming 可以动态控制数据接收速率来适配集群数据处理能力。背压机制（即 Spark Streaming 

Backpressure）: 根据JobScheduler 反馈作业的执行信息来动态调整Receiver 数据接收率。通过属性“spark.streaming.backpressure.enabled”来控制是否启用 backpressure 机制，默认值 false，即不启用。

# Dstream 入门

## 需求

使用 netcat 工具向 9999 端口不断的发送数据，通过 SparkStreaming 读取端口数据并统计不同单词出现的次数

## 实现

添加依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.12</artifactId>
    <version>3.0.0</version>
</dependency>
```

代码

```scala
package com.stanlong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次

        // 逻辑处理
        // 获取端口数据
        val lines = ssc.socketTextStream("127.0.0.1", 9999) // 连接本地9999端口
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map((_, 1))
        val wordCount = wordToOne.reduceByKey(_ + _)
        wordCount.print()

        // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        // main方法执行完毕后，应用程序也会自动结束，所以不能让main方法执行完毕
        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

}
```

## 测试方法

先启动 netcat， 再启动程序。程序启动后通过cmd界面发送数据

![](./doc/71.png)

# DStream创建

## RDD 队列

测试过程中，可以通过使用 ssc.queueStream(queueOfRDDs)来创建 DStream，每一个推送到这个队列中的RDD，都会作为一个DStream 处理。

### 需求

循环创建几个 RDD，将RDD 放入队列。通过 SparkStream 创建 Dstream，计算 WordCount

```scala
package com.stanlong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次

        // 创建RDD队列
        val rddQueue = new mutable.Queue[RDD[Int]]()

        // 创建QueueInputStream
        val inputStream = ssc.queueStream(rddQueue, false)

        val mappedStream = inputStream.map((_, 1))
        val reduceStream = mappedStream.reduceByKey(_ + _)

        // 打印结果
        reduceStream.print()

        // 1. 启动采集器
        ssc.start()

        // 循环创建并向RDD队列中加入RDD
        for(i <- 1 to 5){
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
```

## 自定义数据源

需要继承Receiver，并实现 onStart、onStop 方法来自定义数据源采集

```scala
package com.stanlong.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次

        val messageDS = ssc.receiverStream(new MyReceiver)
        messageDS.print()


        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }

    /**
     * 自定义数据采集器
     * 1. 继承 Receiver 定义泛型
     * 2. 重写方法
     */
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){

        private var flag = true

        override def onStart(): Unit = {
            new Thread(new Runnable {
                override def run(): Unit = {
                    while (flag){
                        val message = "采集的数据为: " + new Random().nextInt(10).toString
                        store(message)
                        Thread.sleep(500)
                    }
                }
            }).start()
        }

        override def onStop(): Unit = {
            flag = false
        }
    }

}
```

## Kafka 数据源

**Kafka 0-10 Direct模式**：是由计算的Executor 来主动消费Kafka 的数据，速度由自身控制

### 需求

通过 SparkStreaming 从Kafka 读取数据，并将读取过来的数据做简单计算，最终打印到控制台

### 实现

导入依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
    <version>3.0.0</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-core</artifactId>
    <version>2.10.1</version
</dependency>
```

代码实现

```scala
package com.stanlong.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        // 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkString")
        val ssc = new StreamingContext(sparkConf, Seconds(3)) // (环境配置， 采集周期)  这里设置每3秒采集一次


        //3.定义 Kafka 参数
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "node02:9092,node03:9092,node04:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "spark-kafka",
            "key.deserializer" ->  "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )


        val kafkaDataDS = KafkaUtils.createDirectStream[String, String]( // k,v都是String类型
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("spark-kafka"), kafkaPara)
        ) // spark-kafka 为 topic名称

        kafkaDataDS.map(_.value()).print()


        // 1. 启动采集器
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
```

### 验证

kafka的相关操作参考  Hadoop/17Kafka

![](./doc/72.png)


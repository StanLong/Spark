package com.stanlong.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark04_SparkSql_Pro {

    def main(args: Array[String]): Unit = {
        // 创建SparkSQl的运行环境
        val sparkSQLConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().enableHiveSupport().config(sparkSQLConf).getOrCreate() // enableHiveSupport() 启用hive支持
        // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
        import spark.implicits._

        // spark 建表并导入数据，这里我直接在hive上把数据准备好了，不用这种方式
        // spark.sql("use spark")

        // // 数据准备
        // spark.sql(
        //     """
        //       |CREATE TABLE `user_visit_action`(
        //       |`date` string,
        //       |`user_id` bigint,
        //       |`session_id` string,
        //       |`page_id` bigint,
        //       |`action_time` string,
        //       |`search_keyword` string,
        //       |`click_category_id` bigint,
        //       |`click_product_id` bigint,
        //       |`order_category_ids` string,
        //       |`order_product_ids` string,
        //       |`pay_category_ids` string,
        //       |`pay_product_ids` string,
        //       |`city_id` bigint)
        //       |row format delimited fields terminated by '\t'
        //       |""".stripMargin)
//
        // spark.sql(
        //     """
        //       |load data local inpath 'datas/spark-sql/user_visit_action.txt' into table user_visit_action
        //      |""".stripMargin)

        // 查询基本数据
        spark.sql(
            """
              | select count(1) as c1 from city_info
              |""".stripMargin).show()





        spark.close()
    }
}

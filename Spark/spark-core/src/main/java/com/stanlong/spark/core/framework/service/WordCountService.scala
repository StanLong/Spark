package com.stanlong.spark.core.framework.service

import com.stanlong.spark.core.framework.common.TService
import com.stanlong.spark.core.framework.dao.WordCountDao

class WordCountService extends TService{

    private val wordCountDao = new WordCountDao

    def dataAnalysis(): Array[(String, Int)] ={
        val lines = wordCountDao.readFile("datas/1.txt")
        val words = lines.flatMap(_.split(" "))
        val wordToOne = words.map(word => (word, 1))
        val wordToSum = wordToOne.reduceByKey(_ + _)
        val array = wordToSum.collect()
        array

    }

}

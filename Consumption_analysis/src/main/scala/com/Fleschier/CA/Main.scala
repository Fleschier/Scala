package com.Fleschier.CA

import Utils.Tools._
import DataAnalysis._


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit ={

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

//    val ss = SparkSession
//      .builder()
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()

    val infile = args(0) + "acc_histrjn_bks.txt"
    val outfile = args(1) + "res.txt"

    val addressInfo = getAllAddressInfo(sc,args(0) + "all_info_of_Address.txt")

    val preProcessData = groupAndRmRedundant(readAndSplit(sc,infile))

    val newDat = addressConvert(preProcessData,addressInfo)  //convert unicode to chinese

    //sc.parallelize(preProcessData).repartition(1).saveAsTextFile(outfile)
    //getAllAddressName(sc,readAndSplit(sc,infile),outfile)
    aggregateAndSave(sc,newDat,outfile)
    //val test = getPopular(sc,newDat) //RDD would destroy the sequence
  }
}

package com.Fleschier.CA.Utils

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching._



object Tools {

  def readAndSplit(sc:SparkContext,inputFile: String): List[Map[String,String]] = {

    //(.匹配任意单个字符)(注意负号匹配时不能忘)
    val pattern =new Regex("""\"\w+\":\"[(\-)*(\\)*\w+]+(\s)*\"""")

//    val datePattern = new Regex("""\"EFFECTDATE\"\:\".+\"""")

    val res = ListBuffer[Map[String,String]]()
    val data = sc.textFile(inputFile)
      .collect() //this is an action can't be dismissed
      .foreach{x =>
//          val date = datePattern.findAllIn(x)
//            .toList
//              .map{time =>
//                val tmp = time.split(":")
//                Map(tmp(0).substring(1,tmp(0).length - 1).trim() -> tmp(1).substring(1,tmp(1).length - 1).trim())
//              }

          pattern.findAllIn(x)
            .toList
            .foreach{x =>
              val tmp = x.split(":")
              res += Map(tmp(0).substring(1,tmp(0).length - 1).trim() -> tmp(1).substring(1,tmp(1).length - 1).trim())
            }
      }
    res.toList
  }

//  def getAllAddressName(sc:SparkContext,rawData:List[Map[String,String]],outPath:String): Unit ={
//    val res = rawData.filter(_.contains("TOACCOUNT")).map(_.values.toString()).distinct
//    sc.parallelize(res).repartition(1).saveAsTextFile(outPath)
//
//  }

  def getAllAddressInfo(sc:SparkContext,input:String): Map[String,String]={
    val res = mutable.Map[String,String]()

    val test = sc.textFile(input).map(_.split(":")).collect()

    test.foreach{x =>
      res ++= Map(x(0).trim -> x(1).trim)
    }

    res.toMap
  }

  def groupAndRmRedundant(rawData: List[Map[String,String]]): List[(String, List[Map[String,String]])] ={
    val res = ListBuffer[(String, List[Map[String,String]])]()//one tuple for one student

    val tmp = ListBuffer[Map[String,String]]()//all tanc info for one student
    val onetranc = mutable.Map[String,String]() //one tranc info

    rawData.foreach{x =>

      if(x.contains("sno")) {
        res.append((x("sno"),tmp.toList))
        tmp.clear()
        onetranc.clear()
      }
      else{
        //SYSCODE:地点编号  POSCODE:刷卡机编号 TOACCOUNT:消费地点
        if(x.contains("LOGICDATE") || /*x.contains("TRADELOCATION") ||*/
          x.contains("SYSCODE") || x.contains("POSCODE") ||
          x.contains("TOACCOUNT") || x.contains("EFFECTDATE")
          /*||x.contains("CARDBALANCE") || x.contains("TRANCODE") */) {
          onetranc ++= x
        }

        else if(x.contains("TRANAMT") && x("TRANAMT").toInt < 0){
          onetranc ++= x
          tmp += onetranc.toMap
          onetranc.clear()
        }
      }
    }


    res.toList
  }


}
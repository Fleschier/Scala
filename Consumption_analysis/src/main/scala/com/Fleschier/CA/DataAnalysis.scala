package com.Fleschier.CA

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/*
* 统计内容包括：
* 每位学生(区分本科生、研究生)一年的消费总额 --done
* 各项(用餐、洗澡/用水、购物，其他等)消费的数额及比例  --done
* 他的消费数额在全部学生中的比例 --done
* 各位学生正常吃早餐的情况
* 学生最常去的食堂、浴室、超市 --done
* 学生一日三餐平均每顿的花费  --done
* 学生每人洗澡累计次数和时间
* 学生每人购物累计次数和费用  --done
* */
object DataAnalysis {
  //elements contains:LOGICDATE,SYSCODE,POSCODE,TOACCOUNT,TRANAMT
  def aggregateAndSave(sc:SparkContext,
                       newDat: List[(String, List[Map[String,String]])],
                       outfile:String): Unit ={
          //newDat: one Map for one tansaction and the String for the student ID
    //sc.parallelize(newDat).repartition(1).saveAsTextFile(outfile)

    val expense = getAllExpense(newDat).map(x => (x._1,x._2.toMap))
    sc.parallelize(expense).repartition(1).saveAsTextFile(outfile)

//    val avgRes = avgMealExpense(sc,newDat)
//    sc.parallelize(avgRes).repartition(1).saveAsTextFile(outfile)

//    val detailsRes = getShoppingAndOthers(newDat)
//    sc.parallelize(detailsRes).repartition(1).saveAsTextFile(outfile)

//    val shoppingInfo = getShoppingInfo(sc,newDat)
//    sc.parallelize(shoppingInfo).repartition(1).saveAsTextFile(outfile)
  }

  def addressConvert(infile:List[(String, List[Map[String,String]])],addressInfo:Map[String,String]):List[(String, List[Map[String,String]])] ={
      val res = ListBuffer[(String, List[Map[String,String]])]()

      val tmp = ListBuffer[Map[String,String]]()

      infile.foreach{student =>
        student._2.foreach{onetranc =>
          val tmap =mutable.Map[String,String]()
          tmap ++= (onetranc - "TOACCOUNT")
          tmap.put("TOACCOUNT",addressInfo(onetranc("TOACCOUNT")))
          tmp.append(tmap.toMap)
        }
        res.append((student._1,tmp.toList))
        tmp.clear()
      }

      res.toList
    }

  def getPopular(sc:SparkContext,newDat: List[(String, List[Map[String,String]])]): List[(String,Int)] ={
    val res = sc.parallelize(newDat).flatMap(_._2)
      .map(x => (x("TOACCOUNT"),1))
      .reduceByKey(_ + _)
      .collect()
      .toList
      .sortBy(-_._2) //逆序排序，刷卡次数从高到底

    res.foreach(println)

    res
  }

  def getAllExpense(indata: List[(String, List[Map[String,String]])]): List[(String,Map[String,String])] ={
        val personalExpense = ListBuffer[(String,mutable.Map[String,String])]()

        var sum = 0  //the expense for all student

        indata.foreach{x =>
          var persum = 0

          x._2.foreach{tranc =>
            persum += tranc("TRANAMT").toInt
          }
          personalExpense.append((x._1,mutable.Map("消费总额" -> persum.toString)))
          sum += persum
        }

      personalExpense.map{info =>
        info._2.put("消费占比",(info._2("消费总额").toDouble/sum).toString)
      }

      val res = personalExpense.toList.map(x => (x._1,x._2.toMap))

      res.sortBy(_._2("消费总额").toInt).foreach(println) //in the cluster sort is useless

      res
    }

  def avgMealExpense(sc:SparkContext,newDat:List[(String, List[Map[String,String]])]): List[(String,Map[String,String])] ={
    val res = ListBuffer[(String,mutable.Map[String,String])]()

    val mealSites = List("本部方塔餐厅","东区四食堂二楼","本部莘园一楼","东区第五餐厅",
      "本部莘园二楼","东区第七餐厅","东区第四餐厅","独墅湖一期二食堂","北区第六餐厅",
      "阳澄湖校区四食堂(后街小吃)","独墅湖一期三食堂","阳澄湖三食堂","独墅湖二期六食堂",
      "独墅湖一期一食堂","独墅湖二期五食堂","东区塔影饭店","独墅湖二期七食堂","独墅湖二期八食堂",
      "阳澄湖教工食堂")

    val allMeals = sc.parallelize(newDat)
      .map{ student =>
        val tmp = student._2.filter{tranc =>
          mealSites.indexOf(tranc("TOACCOUNT")) >= 0 //if exists in the list
        }
        (student._1,tmp)
      }.collect()

    //println(allMeals.count())

    allMeals.foreach{trancs =>
      val count = trancs._2.length //all meals for a student
      var totalexpense = 0  //total expense on meals
      trancs._2.foreach{onetranc =>
        totalexpense += onetranc("TRANAMT").toInt

      }
      val avgexpense = totalexpense.toDouble/count
      res.append((trancs._1,mutable.Map("食堂消费总额" -> totalexpense.toString, "平均每餐花费" -> avgexpense.toString)))
    }

    res.map(x => (x._1,x._2.toMap)).toList
  }

  def getShoppingAndOthers(newDat:List[(String, List[Map[String,String]])]):List[(String,List[Map[String,String]])] ={

    val res = ListBuffer[(String,List[Map[String,String]])]()

    val mealSites = List("本部方塔餐厅","东区四食堂二楼","本部莘园一楼","东区第五餐厅",
      "本部莘园二楼","东区第七餐厅","东区第四餐厅","独墅湖一期二食堂","北区第六餐厅",
      "阳澄湖校区四食堂(后街小吃)","独墅湖一期三食堂","阳澄湖三食堂","独墅湖二期六食堂",
      "独墅湖一期一食堂","独墅湖二期五食堂","东区塔影饭店","独墅湖二期七食堂","独墅湖二期八食堂",
      "阳澄湖教工食堂")
    val waterSites = List("学校刷卡开水炉(后勤管理处)","独墅湖一期宿舍水控","独一期宿舍水控(东吴)","阳澄湖水控商户")
    val bathSites = List("东校区浴室","本部浴室收费","本部女生大院浴室","本部13号楼浴室")
    val shopSites = List("东区教育超市","本部第一餐厅二楼教育超市","北区教育超市",
      "阳澄湖教育超市","独墅湖一期教育超市","独墅湖二期校园超市","阳澄湖大星星(经营部)",
      "独墅湖一期四季水果超市","独墅湖二期金苹果超市","怡家乐超市苏大独墅湖二期加盟店",
      "独墅湖二期华联超市","独墅湖一期学友超市","独墅湖二期信民电子商店")
    val otherSites = List("电瓶车自助充电","本部图书馆出版社读者服务部","东区出版社读者服务部书店",
    "学生成绩自助打印(教务处)","图书馆","信息化建设与管理中心","苏州海奥斯贸易有限公司","体育学院场馆刷卡")

    newDat.foreach{student =>
      var persum = 0 //all expenses for one student
      var mealExpense = 0
      var waterExpense = 0
      var bathExpense = 0
      var shoppingExpense = 0
      var otherExpense = 0

      student._2.foreach{tranc =>
        persum += tranc("TRANAMT").toInt

        if(waterSites.indexOf(tranc("TOACCOUNT")) >= 0){
          waterExpense += tranc("TRANAMT").toInt
        }
        else if(bathSites.indexOf(tranc("TOACCOUNT")) >= 0){
          bathExpense += tranc("TRANAMT").toInt
        }
        else if(shopSites.indexOf(tranc("TOACCOUNT")) >= 0){
          shoppingExpense += tranc("TRANAMT").toInt
        }
        else if(otherSites.indexOf(tranc("TOACCOUNT")) >= 0){
          otherExpense += tranc("TRANAMT").toInt
        }
        else if(mealSites.indexOf(tranc("TOACCOUNT")) >= 0){
          mealExpense += tranc("TRANAMT").toInt
        }
      }

      val mealPercent = mealExpense.toDouble/persum
      val waterPercent = waterExpense.toDouble/persum
      val bathPercent = bathExpense.toDouble/persum
      val shoppingPercent = shoppingExpense.toDouble/persum
      val otherPercent = otherExpense.toDouble/persum


      res.append((student._1,List(Map("消费总额" -> persum.toString),
        Map("食堂消费总额" -> mealExpense.toString),Map("食堂消费占比" -> mealPercent.toString),
        Map("洗澡消费总额" -> bathExpense.toString),Map("洗澡消费占比" -> bathPercent.toString),
        Map("购物消费总额" -> shoppingExpense.toString),Map("购物消费占比" -> shoppingPercent.toString),
        Map("其他消费总额" -> otherExpense.toString),Map("其他消费占比" -> otherPercent.toString))))

    }

    res.toList
  }

  def getShoppingInfo(sc:SparkContext,newDat:List[(String, List[Map[String,String]])]):List[(String,Map[String,String])] ={
    val res = ListBuffer[(String,mutable.Map[String,String])]()

    val shopSites = List("东区教育超市","本部第一餐厅二楼教育超市","北区教育超市",
      "阳澄湖教育超市","独墅湖一期教育超市","独墅湖二期校园超市","阳澄湖大星星(经营部)",
      "独墅湖一期四季水果超市","独墅湖二期金苹果超市","怡家乐超市苏大独墅湖二期加盟店",
      "独墅湖二期华联超市","独墅湖一期学友超市","独墅湖二期信民电子商店")

    val allShoppingInfo = sc.parallelize(newDat)
      .map{ student =>
        val tmp = student._2.filter{tranc =>
          shopSites.indexOf(tranc("TOACCOUNT")) >= 0 //if exists in the list
        }
        (student._1,tmp)
      }.collect()

    allShoppingInfo.foreach{student =>
      var allExpense = 0    //shopping expense
      val counts = student._2.length  //shopping times
      student._2.foreach{tranc =>
        allExpense += tranc("TRANAMT").toInt
      }
      val avgExpense = allExpense.toDouble/counts
      res.append((student._1,mutable.Map("消费总额" -> allExpense.toString,
        "消费次数" -> counts.toString,"平均每次消费金额" -> avgExpense.toString)))
    }

    res.map(x => (x._1,x._2.toMap)).toList
  }

}

package com.hainiu.spark.utils

import java.util

import com.hainiu.spark.utils.extractor.HtmlContentExtractor
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object JsoupXpathDemo {
  def main(args: Array[String]): Unit = {

    // 测试提取单个url正文
//    val url:String = "http://www.bjnews.com.cn/inside/2017/07/15/450512.html"
//    val url:String = "http://www.chinanews.com/gn/2020/05-17/9186863.shtml"
    val url:String = "http://www.chinanews.com/gn/2020/05-18/9187222.shtml"
    getXpathAndExtract(url)


    // 测试批量提取url正文
//    getBatchXpathAndExtract()

  }

  /**
    * 测试批次提取xpath和抽取正文
    * 主要看反xpath的算法
    */
  def getBatchXpathAndExtract(): Unit ={
    val list = new ListBuffer[String]
    list += "http://www.bjnews.com.cn/inside/2017/07/15/450512.html"
    list += "https://www.sohu.com/a/390639308_100034414"
    list += "https://finance.sina.com.cn/roll/2020-05-13/doc-iircuyvi2911789.shtml"
    list += "https://taiwan.huanqiu.com/article/9CaKrnKqYH6"
    list += "http://bj.people.com.cn/n2/2020/0516/c14540-34022009.html"

    for(url <- list){
      getXpathAndExtract(url)
      println("------------------------------")

    }
  }

  def getXpathAndExtract(url:String) = {
//    val url = "https://www.sohu.com/a/390639308_100034414"
//    val url = "https://finance.sina.com.cn/roll/2020-05-13/doc-iircuyvi2911789.shtml"
//    val url = "http://www.bjnews.com.cn/inside/2017/07/15/450512.html"
//    val url = "https://taiwan.huanqiu.com/article/9CaKrnKqYH6"
//    val url = "http://bj.people.com.cn/n2/2020/0516/c14540-34022009.html"
    val xpathMap: util.Map[String, String] = HtmlContentExtractor.generateXpath2(url)
    //		Map<String, String> stringStringMap = HtmlContentExtractor.generateXpath2("http://www.bjnews.com.cn/inside/2017/07/15/450512.html");
    System.out.println(xpathMap)

    // 正规则xpath集合
    val tset = new mutable.HashSet[String]()
    // 反规则xpath集合
    val fset = new mutable.HashSet[String]()

    if (JavaUtil.isNotEmpty(xpathMap)) {
      val it: util.Iterator[util.Map.Entry[String, String]] = xpathMap.entrySet().iterator()
      while (it.hasNext) {
        val entry: util.Map.Entry[String, String] = it.next()
        val key = entry.getKey
        val value: String = entry.getValue
        println(s"key:${key},value:${value}")

        //正文xpath
        if(value.equals(HtmlContentExtractor.CONTENT)){
          tset.add(key)
        }else{
          // 反规则xpath
          fset.add(key)
        }

      }
    }

    println(s"正规则xpath：${tset}")
    println(s"反规则xpath：${fset}")

    val doc: Document = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get()

    // 正文提取
    val context: String = Util.getContext(doc, tset, fset)
    println(context)

  }
}


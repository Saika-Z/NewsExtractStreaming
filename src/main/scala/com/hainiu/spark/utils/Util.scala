package com.hainiu.spark.utils

import java.text.SimpleDateFormat

import cn.wanghaomiao.xpath.model.JXDocument
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import scala.collection.mutable.{HashSet, ListBuffer}
import java.util
import java.util.Date

import org.jsoup.Jsoup

import scala.collection.mutable
import scala.io.BufferedSource

object Util {
  /**
    * 根据正反规则的xpath从dom中抽取正文
    * 从java中移植过来的
    */
  def getContext(doc: Document, positiveXpath: HashSet[String], negativeXpath: HashSet[String]): String = {
    val jx: JXDocument = new JXDocument(doc)
    // 获取正规则的节点列表
    val positiveElementList: List[Elements] = getElementsList(jx, positiveXpath)
    // 获取反规则的节点列表
    val negativeElementList: List[Elements] = getElementsList(jx, negativeXpath)

    //将所有反规则的元素从dom中移除
    try {
      for (elements: Elements <- negativeElementList) {
//        val value: Array[Element] = elements.toArray().asInstanceOf[Array[Element]]
        val value = elements.iterator()
        while (value.hasNext) {
          val element: Element = value.next()
          try{
            element.remove()
          }catch{
            // 防止重复删除报错
            case ce: IllegalArgumentException => println("删除没有的节点报错忽略")

          }

        }
      }
    } catch {
      case e: ClassCastException => e.printStackTrace()
    }

    // 获取正规则节点的内容
    var context: String = ""
    for (elements: Elements <- positiveElementList) {
      val text: String = elements.text()
      if (text != null) context += text
    }

    context
  }

  /**
    * 拿取正反规则xpath匹配到的标签
    *
    * @param jx               根据你的Html生成的JXDocument对象
    * @param xpathSet         xpath集合
    * @return                 匹配到的标签集合
    */
  private def getElementsList(jx: JXDocument, xpathSet: mutable.HashSet[String]): List[Elements] = {
    val list: ListBuffer[Elements] = new ListBuffer[Elements]

    if (xpathSet == null || xpathSet.isEmpty) {
      list.toList
    } else {
      for (xpath <- xpathSet) {
        val sel = jx.sel(xpath).asInstanceOf[util.List[Element]]
        val eles: Elements = new Elements(sel)
        list += eles
      }
      list.toList
    }
  }

  def getCurrentTime: String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    dateFormat.format(now)
  }

  def getTime(time: Long, pattern: String): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(pattern)
    dateFormat.format(time)
  }

  def main(args: Array[String]): Unit = {
//    println(getCurrentTime)

////    println(getTime(new Date().getTime, "yyyy-MM-dd"))
//    val url:String = "http://www.bjnews.com.cn/inside/2017/07/15/450512.html"
//    val doc: Document = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20100101 Firefox/29.0").get()
//
//    val tset = new mutable.HashSet[String]()
//    tset.add("//div[@id='main']/div[@class='fl nleft']/div[@class='content']/p")
////
//    val fset = new mutable.HashSet[String]()
//    fset.add("//div[@id='main']/div[@class='fl nleft']/div[@class='content']/p/span")
////    fset.add("//*[@id=\"main\"]/div[1]/div[5]/p[32]")
//    val context: String = Util.getContext(doc, tset, fset)
//    println(context)

  }
}
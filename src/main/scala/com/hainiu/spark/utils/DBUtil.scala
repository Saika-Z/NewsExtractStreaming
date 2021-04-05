package com.hainiu.spark.utils


import java.sql.{Connection, Statement, Timestamp}
import java.util.Date

import org.apache.commons.codec.digest.DigestUtils

import scala.collection.mutable.HashMap


object DBUtil {

  def insertIntoMysqlByJdbc(scanAccMap:HashMap[Any,Any],filteredAccMap:HashMap[Any,Any],extractAccMap:HashMap[Any,Any],emptyAccMap:HashMap[Any,Any],notMatchAccMap:HashMap[Any,Any]){
    if(scanAccMap.size !=0){
      var connection:Connection = null
      var statement:Statement = null
      try{
        val date = new Date()
        val time:Long = date.getTime
        val timestamp = new Timestamp(time)
        val hour_md5:String = DigestUtils.md5Hex(Util.getTime(time,"yyyyMMddHH"))
        val scanDay:Int = Util.getTime(time,"yyyyMMdd").toInt
        val scanHour:Int = Util.getTime(time,"HH").toInt
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        for((host,num) <- scanAccMap){
          val scanNum = num
          val filteredNum:Long = filteredAccMap.getOrElse(host,0L).asInstanceOf[Long]
          val extractNum:Long = extractAccMap.getOrElse(host,0L).asInstanceOf[Long]
          val emptyNum:Long = emptyAccMap.getOrElse(host,0L).asInstanceOf[Long]
          val notMatchNum:Long = notMatchAccMap.getOrElse(host,0L).asInstanceOf[Long]
          val sql =
            s"""
               |insert into c20_zhaowq_stream_extract_xpath_rule
               |(host, scan, filtered, extract, empty_context, no_match_xpath, scan_day, scan_hour,scan_time, hour_md5)
               |values('$host',$scanNum,$filteredNum,$extractNum,$emptyNum,$notMatchNum,$scanDay,$scanHour,'$timestamp','$hour_md5')
               |on DUPLICATE_KEY_UPDATE
               |scan=scan+$scanNum,filtered=filtered+$filteredNum,extract=extract+$extractNum,
               |empty_context=empty_context+$emptyNum,no_match_xpath=no_match_xpath+$notMatchNum;
             """.stripMargin
          statement.execute(sql)
        }
        connection.commit()
      }catch {
        case e:Exception =>{
          e.printStackTrace()
          try{
            connection.rollback()
          }catch {
            case e:Exception =>e.printStackTrace()
          }
        }
      }finally {
        try{
          if(statement!=null) statement.close()
          if(connection!=null) connection.close()
        }catch {
          case e:Exception =>e.printStackTrace()
        }
      }
    }
  }

}

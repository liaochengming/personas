package com.kunyan.userportrait.temp.lcm.crawler.weibo

import java.util.regex.Pattern

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by lcm on 2016/5/10.
  * 用于启动爬取数据的程序
  */
object Controller {

  var ips = new ListBuffer[String]

  /**
    * @param args 输入的数据文件和输出的数据文件
    */
  def main(args: Array[String]) {

    //保存微博的用户id
    var uaAndUidSet = new HashSet[(String, String)]

    //切换代理ip
    ips = FetchIP.getIPs
    changIP()

    //读取源数据文件
    for (line <- Source.fromFile(args(0))("UTF-8").getLines()) {

      val lineArr = line.split("\\t")
      val url = lineArr(0)
      val ref = lineArr(1)
      val pattern = Pattern.compile("weibo.c(n|om)/u?/?(.*uid=)?(\\d{7,10})")
      val url_m1 = pattern.matcher(url)
      var url_uid = ""

      //匹配url的uid
      if (url_m1.find()) {

        url_uid = url_m1.group(3)
        uaAndUidSet.+=((lineArr(2), url_uid))

      }

      //匹配ref的uid
      val ref_m1 = pattern.matcher(ref)

      if (ref_m1.find()) {

        val ref_uid = ref_m1.group(3)
        if (url_uid != ref_uid) uaAndUidSet.+=((lineArr(2), ref_uid))

      }
    }
    //爬取微博信息并保存
    NewWeiBoCrawler.crawlWeiBoInfo(uaAndUidSet, args(1))
  }

  /**
    * 用来改变代理IP
    */
  def changIP(): Unit = {

    val ipPort = ips((Math.random() * (ips.length - 10)).toInt).split(":")
    val ip = ipPort(0)
    val port = ipPort(1)
    System.getProperties.setProperty("http.proxyHost", ip)
    System.getProperties.setProperty("http.proxyPort", port)

  }
}



package com.kunyan.userportrait.temp.lcm.crawler.O2O

import java.io.IOException
import java.net.{SocketException, SocketTimeoutException, UnknownHostException}

import org.jsoup.{HttpStatusException, Jsoup}

/**
 * Created by lcm on 2016/5/20.
 * 用于获取代理ip
 * 并且切换ip
 */
object ProxyIP {

  val ua = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36 QIHU 360SE se.360.cn"

  var ips: Array[String] = null

  /**
   * 用于获取代理IP
   */
  def getIPs() {

    try {

      val doc = Jsoup.connect("http://qsdrk.daili666api.com/ip/?tid=558465838696598&num=500&delay=5&foreign=none&ports=80,8080")
        .userAgent(ua)
        .timeout(30000)
        .followRedirects(true)
        .execute()

      ips = doc.body().split("\r\n")

    } catch {

      case ex: HttpStatusException => ex.printStackTrace()
      case ex: SocketTimeoutException => ex.printStackTrace()
      case ex: SocketException => ex.printStackTrace()
      case ex: UnknownHostException =>
        ex.printStackTrace()
        changIP()
      case ex: IOException => ex.printStackTrace()

    }
  }


  /**
   * 用来改变代理IP
   */
  def changIP(): Unit = {

    if (ips != null) {

      val ipPort = ips((Math.random() * (ips.length - 5)).toInt).split(":")
      val ip = ipPort(0)
      val port = ipPort(1)
      System.getProperties.setProperty("http.proxyHost", ip)
      System.getProperties.setProperty("http.proxyPort", port)

    }
  }
}

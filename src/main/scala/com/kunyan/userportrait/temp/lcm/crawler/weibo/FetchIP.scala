package com.kunyan.userportrait.temp.lcm.crawler.weibo

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL, URLConnection}
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable.ListBuffer

/**
  * Created by lcm on 2016/7/22.
  * 提取可用代理IP
  */
object FetchIP {

  val ips = new ListBuffer[String]

  def getIPs: ListBuffer[String] = {

    for (i <- 0 to 20) {

      val ipStr = doHttp("http://qsdrk.daili666api.com/ip/?tid=558465838696598&num=500&delay=5&foreign=none&ports=80,8080", 3000)

      if (ipStr != "false") {

        val ipPort = ipStr.split("\\t")

        for (ip <- ipPort) ips.+=(ip)

      }
    }
    println(ips.size)

    //创建一个可重用固定线程数的线程池
    val pool = Executors.newFixedThreadPool(20)

    for (index <- ips.indices) {

      val task = new Thread(new Runnable {
        override def run(): Unit = {

          println(index)
          val ipPort = ips(index).split(":")

          if (ipPort.length == 2) {

            val ip = ipPort(0)
            val port = ipPort(1)
            System.getProperties.setProperty("http.proxyHost", ip)
            System.getProperties.setProperty("http.proxyPort", port)
            val wbHtml = doHttp("http://weibo.com/u/5973789546?refer_flag=1008085010_&is_all=1#_loginLayer_1469157929660", 500)

            if (wbHtml == "false") ips.remove(index)

          }
        }
      })

      pool.submit(task)

    }
    pool.shutdown()
    var over = false

    while (!over) over = pool.awaitTermination(2000, TimeUnit.MILLISECONDS)

    ips
  }


  def doHttp(url: String, time: Int): String = {

    val localURL: URL = new URL(url)
    val ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.86 Safari/537.36"

    val connection: URLConnection = localURL.openConnection
    val httpURLConnection: HttpURLConnection = connection.asInstanceOf[HttpURLConnection]

    httpURLConnection.setConnectTimeout(time)
    httpURLConnection.setReadTimeout(time)

    httpURLConnection.setRequestProperty("Connection", "Keep-Alive")
    httpURLConnection.setRequestProperty("Accept-Charset", "utf-8")
    httpURLConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    httpURLConnection.setRequestProperty("User-Agent", ua)

    var inputStream: InputStream = null
    var inputStreamReader: InputStreamReader = null
    var reader: BufferedReader = null
    val resultBuilder: StringBuilder = new StringBuilder
    var tempLine: String = null

    try {

      if (httpURLConnection.getResponseCode == 200) {

        inputStream = httpURLConnection.getInputStream
        inputStreamReader = new InputStreamReader(inputStream)
        reader = new BufferedReader(inputStreamReader)

        while ( {
          tempLine = reader.readLine
          tempLine
        } != null) resultBuilder.append(tempLine + "\t")

      }else{

        resultBuilder.append("false")

      }

    } catch {

      case e: IOException => resultBuilder.append("false")
      case e: Exception => resultBuilder.append("false")

    } finally {

      if (reader != null) reader.close()
      if (inputStreamReader != null) inputStreamReader.close()
      if (inputStream != null) inputStream.close()

    }


    resultBuilder.toString
  }

}

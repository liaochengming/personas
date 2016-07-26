package com.kunyan.userportrait.temp.lcm.crawler.weibo

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL, URLConnection}
import java.sql.{DriverManager, SQLException}
import java.util
import java.util.concurrent.{Executors, TimeUnit}
import java.util.regex.Pattern

import org.json.JSONObject
import org.jsoup.Jsoup

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by lcm on 2016/7/20.
  * 最新的微博信息爬取程序
  */
object NewWeiBoCrawler {

  var cookies = new ListBuffer[String]

  /**
    * 控制微博数据的爬取和保存
    * @param data ua和微博的用户ID
    * @param cookieFile 有cookie的文件路径
    */
  def crawlWeiBoInfo(data: HashSet[(String, String)], cookieFile: String): Unit = {

    cookies = getCookies(cookieFile)

    //获取微博用户信息
    val weiBoInfo = getWeiBoInfo(data)

    //保存用户信息
    saveWeiBoInfo(weiBoInfo)

  }


  /**
    * 控制爬取用户信息
    * @param data ： 用于爬取用户信息的id集合
    * @return ：微博的用户信息集合
    */
  def getWeiBoInfo(data: HashSet[(String, String)]): HashSet[String] = {

    var weiBoInfo = new HashSet[String]

    //创建一个可重用固定线程数的线程池
    val pool = Executors.newFixedThreadPool(30)

    for (uaAndUid <- data) {

      Controller.changIP()

      val task = new Thread(new Runnable {
        override def run(): Unit = {

          //用url获取id
          val urlForPageId = "http://weibo.com/u/" + uaAndUid._2
          val htmlForPageId = doHttp(urlForPageId, uaAndUid._1, getCookieStr)
          val pageId: String = parseHtmlFoWeiBoPageId(htmlForPageId)

          if (pageId != "") {

            var htmlForUserInfo: String = ""
            var info: String = ""
            val urlForUserInfo = "http://weibo.com/p/" + pageId + "/info?mod=pedit_more"

              htmlForUserInfo = doHttp(urlForUserInfo, uaAndUid._1, getCookieStr)
              info = parseHtmlForUserInfo(htmlForUserInfo, pageId)

            if (info != "") weiBoInfo = weiBoInfo.+(info)

          }

        }
      })

      pool.submit(task)

    }

    pool.shutdown()
    var over = false
    while (!over) over = pool.awaitTermination(2000, TimeUnit.MILLISECONDS)
    weiBoInfo

  }

  /**
    * 读取cookie文件获取cookies
    * @param cookieFile cookie文件的路径
    * @return 读取的cookies
    */
  def getCookies(cookieFile: String): ListBuffer[String] = {

    val cookies = new ListBuffer[String]

    for (line <- Source.fromFile(cookieFile).getLines()) cookies.+=(line)

    cookies

  }

  /**
    * 将cookie字符串转成map
    * 根据cookie字符串获取cookie的map
    */
  def getCookieStr: String = {

    val cookieStr = cookies((Math.random() * cookies.size).toInt)
    cookieStr
  }

  /**
    * 此方法根据html字符串解析微博页面ID
    *
    * @param html 微博用户页面
    * @return 微博用户页面ID
    */
  def parseHtmlFoWeiBoPageId(html: String): String = {

    var pageId = ""
    val doc = Jsoup.parse(html)
    val pattern = Pattern.compile("weibo.com\\\\/p\\\\/(\\d+)")
    val matcher = pattern.matcher(doc.body().toString)

    if (matcher.find()) pageId = matcher.group(1)

    pageId
  }


  /**
    * 此方法解析html数据
    * @param html html字符串
    * @param id   微博的页面ID
    * @return 解析后的用户信息
    */
  def parseHtmlForUserInfo(html: String, id: String): String = {

    //    微博账号
    val weiBoId = id.substring(6)

    //QQ
    var QQ = ""

    //邮箱
    var email = ""

    //职业(不可获得)
    val job = ""

    //身份
    var position = ""

    //真是姓名(不可获得)
    val realName = ""

    //公司
    var company = ""

    //地址（所在地）
    var address = ""

    val infoList = List("QQ：", "邮箱：", "所在地：", "公司：", "职位：")
    val doc = Jsoup.parse(html)
    val docChildren = doc.body().children()

    for (i <- 0 until docChildren.size()) {

      val child = docChildren.get(i).data()

      if (child.contains("\"domid\":\"Pl_Official_PersonalInfo__62")) {

        val jsonObjectData = new JSONObject(child.substring(8, child.length - 1))
        val dataHtml = jsonObjectData.getString("html")
        val dataDoc = Jsoup.parse(dataHtml)
        val dataNodes = dataDoc.body().children().tagName("div")
        val dataNodesIt = dataNodes.iterator()

        while (dataNodesIt.hasNext) {

          val dataNode = dataNodesIt.next()
          val whichInfo = dataNode.childNode(1).childNode(1).childNode(1).childNode(0).childNode(0).toString

          whichInfo match {

            case "基本信息" =>
              val basicInfo = dataNode.childNode(1).childNode(3).childNode(1).childNode(1).childNodes()
              for (j <- 0 until basicInfo.size()) {

                val infoNode = basicInfo.get(j)

                if (infoNode.nodeName() == "li") {

                  val infoChildNodes = infoNode.childNodes
                  val it = infoChildNodes.iterator()

                  var title = ""
                  var detail = ""

                  while (it.hasNext) {

                    val node = it.next()

                    if (node.attributes().get("class") == "pt_title S_txt2") title = node.childNodes().get(0).toString

                    if (node.attributes().get("class") == "pt_detail") detail = node.childNodes().get(0).toString.trim

                  }

                  if (infoList.contains(title)) address = detail
                }
              }

            case "联系信息" =>
              val contactInfo = dataNode.childNode(1).childNode(3).childNode(1).childNode(1).childNodes()
              val it = contactInfo.iterator()
              while (it.hasNext) {

                val contactNode = it.next()
                if (contactNode.nodeName() == "li") {

                  val contactChildNode = contactNode.childNodes()
                  var title = ""
                  var detail = ""

                  if (contactChildNode.size() == 2) {

                    title = contactNode.childNode(0).childNode(0).toString
                    detail = contactNode.childNode(1).childNode(0).toString

                  }
                  if (contactChildNode.size() == 5) {

                    title = contactNode.childNode(1).childNode(0).toString
                    detail = contactNode.childNode(3).childNode(0).toString

                  }

                  if (infoList.contains(title)) {

                    title match {

                      case "QQ：" => QQ = detail
                      case "邮箱：" => email = detail

                    }
                  }
                }
              }

            case "工作信息" =>
              val workInfo = dataNode.childNode(1).childNode(3).childNode(1).childNode(1).childNodes()

              for (k <- 0 until workInfo.size()) {

                val workNode = workInfo.get(k)

                if (workNode.nodeName() == "li") {

                  val infoChildNodes = workNode.childNodes
                  val it = infoChildNodes.iterator()

                  while (it.hasNext) {

                    val node = it.next()
                    if (node.attributes().get("class") == "pt_detail") {

                      val nodeSize = node.childNodes().size()
                      company = node.childNodes().get(1).childNodes().get(0).toString
                      var positionMessage = ""

                      if (nodeSize == 7) {

                        positionMessage = node.childNodes().get(6).toString

                        if (positionMessage != " ") position = positionMessage.substring(4).trim

                      }
                      if (nodeSize == 5) {

                        positionMessage = node.childNodes().get(4).toString

                        if (positionMessage != " " && position == "") position = positionMessage.substring(4).trim

                      }
                    }
                  }
                }
              }
            case "教育信息" =>
            case "标签信息" =>
          }
        }
      }
    }

    if (address != "") {

      weiBoId + "-->" + QQ + "-->" + email + "-->" + job + "-->" + position + "-->" + realName + "-->" + company + "-->" + address

    } else ""
  }


  /**
    * http请求数据
    *
    * @param url http网址
    * @param ua 请求所需参数ua
    * @param cookie 请求所需参数cookie
    * @return 请求结果字符串
    */
  def doHttp(url: String, ua: String, cookie: String): String = {

    val localURL: URL = new URL(url)

    val connection: URLConnection = localURL.openConnection
    val httpURLConnection: HttpURLConnection = connection.asInstanceOf[HttpURLConnection]

    httpURLConnection.setConnectTimeout(1000)
    httpURLConnection.setReadTimeout(1000)

    httpURLConnection.setRequestProperty("Connection", "keep-alive")
    httpURLConnection.setRequestProperty("Accept-Charset", "utf-8")
    httpURLConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    httpURLConnection.setRequestProperty("Cookie", cookie)
    httpURLConnection.setRequestProperty("User-Agent", ua)

    var inputStream: InputStream = null
    var inputStreamReader: InputStreamReader = null
    var reader: BufferedReader = null
    val resultBuilder: StringBuilder = new StringBuilder
    var tempLine: String = null

    try {

      inputStream = httpURLConnection.getInputStream
      inputStreamReader = new InputStreamReader(inputStream)
      reader = new BufferedReader(inputStreamReader)

      while ( {

        tempLine = reader.readLine
        tempLine

      } != null) {

        resultBuilder.append(tempLine)

      }
    } catch {

      case e: IOException => e.printStackTrace()
      case e: Exception => e.printStackTrace()

    } finally {

      if (reader != null) reader.close()
      if (inputStreamReader != null) inputStreamReader.close()
      if (inputStream != null) inputStream.close()

    }


    resultBuilder.toString
  }


  /**
    * 用于保存微博信息
    *
    * @param weiBoInfo ：微博用于的信息集合
    */
  def saveWeiBoInfo(weiBoInfo: HashSet[String]): Unit = {

    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = DriverManager.getConnection(AccountInfo.conn_str)
    val statement = conn.createStatement()
    val wb_weiBoResultSet = statement.executeQuery("SELECT weibo_id FROM weibo")

    //获取微博表中微博id,并保存到集合
    var wbWeiBoIdSet = new HashSet[String]

    while (wb_weiBoResultSet.next()) {

      val weiBoId = wb_weiBoResultSet.getString("weibo_id")
      wbWeiBoIdSet = wbWeiBoIdSet.+(weiBoId)

    }

    val main_weiBoResultSet = statement.executeQuery("SELECT id,weibo FROM main_index where weibo<> \"\"")

    //获取main_index中的微博id,并保存到map
    val mainWeiBoIdMap = new util.HashMap[String, Int]
    while (main_weiBoResultSet.next()) {

      val id = main_weiBoResultSet.getInt("id")
      val weiBo = main_weiBoResultSet.getString("weibo")
      mainWeiBoIdMap.put(weiBo, id)

    }

    for (infoStr <- weiBoInfo) {

      val infoArr = infoStr.split("-->")
      val uid = infoArr(0)

      if (infoArr.length == 8) {

        //先判断主表有没有weibo的用户id,如果没有，则添加到主表中并返回微博id在主表的id号
        if (!mainWeiBoIdMap.containsKey(uid)) {

          try {

            val prep = conn.prepareStatement("INSERT INTO main_index (phone,qq,weibo) VALUES (?, ?, ?) ")
            prep.setString(1, "")
            prep.setString(2, infoArr(1))
            prep.setString(3, uid)
            prep.executeUpdate

          } catch {

            case ex: SQLException => ex.printStackTrace()
          }

          val result = statement.executeQuery("SELECT id FROM main_index where weibo='" + uid + "'")
          var main_index_id = 0

          if (result.next()) main_index_id = result.getInt("id")

          mainWeiBoIdMap.put(uid, main_index_id)

        } else {

          //如果qq不为空，判断qq是否需要更新
          if (infoArr(1) != "") {

            val result = statement.executeQuery("SELECT qq FROM main_index where weibo=" + uid)
            var qq = ""
            if (result.next()) qq = result.getString("qq")

            if (infoArr(1) != qq) {

              try {

                val prep = conn.prepareStatement("update main_index set qq=? where weibo=?")
                prep.setString(1, infoArr(1))
                prep.setString(2, uid)
                prep.executeUpdate

              } catch {

                case ex: SQLException => ex.printStackTrace()
              }
            }
          }
        }

        if (!wbWeiBoIdSet.contains(uid)) {
          try {

            //将数据写到weiBo表中
            val prep = conn.prepareStatement("INSERT INTO weibo (main_index_id, weibo_id,qq,email,job,position,realName,company,address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ")
            prep.setInt(1, mainWeiBoIdMap.get(uid))
            prep.setString(2, uid)
            prep.setString(3, infoArr(1))
            prep.setString(4, infoArr(2))
            prep.setString(5, infoArr(3))
            prep.setString(6, infoArr(4))
            prep.setString(7, infoArr(5))
            prep.setString(8, infoArr(6))
            prep.setString(9, infoArr(7))
            prep.executeUpdate

          } catch {

            case ex: SQLException => ex.printStackTrace()

          }
        }
      }
    }

    conn.close()
  }
}

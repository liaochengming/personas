package com.kunyan.userportrait.temp.lcm.crawler.O2O

import java.io.IOException
import java.sql.{DriverManager, SQLException}
import java.util
import java.util.concurrent.{Executors, Future, TimeUnit}

import com.kunyan.userportrait.temp.lcm.crawler.weibo.AccountInfo
import org.json.JSONObject
import org.jsoup.Connection.Method
import org.jsoup.Jsoup

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * Created by lcm on 2016/5/21.
 * 此类爬取百度外卖的用户信息
 */
object CrawlerBaiDu {

  def main(args: Array[String]) {

    //获取代理ip
    ProxyIP.getIPs()
    ProxyIP.changIP()

    var infoSet = new HashSet[String]
    //获取可爬取的ua和cookie
    val uaAndCookie = getData(args(0))

    //创建一个可重用固定线程数的线程池
    val pool = Executors.newFixedThreadPool(12)

    var listFu = new ListBuffer[Future[HashSet[String]]]

    for (uc <- uaAndCookie) {

      val task = new CrawlerBaiDuTask(uc._1, uc._2)
      val fu = pool.submit(task)
      listFu.+=(fu)

    }

    for (f <- listFu) {
      try {

        f.get(3000, TimeUnit.MILLISECONDS).foreach(info => {

          infoSet.+=(info)

        })

      } catch {

        case ex: Exception => f.cancel(true)

      } finally {

        pool.shutdown()

      }

    }

    saveBDInfo(infoSet)

  }

  /**
   * 将cookie字符串转成map
   * 根据cookie字符串获取cookie的map
   */
  def getCookieMap(cookie: String): util.HashMap[String, String] = {

    val cookieMap = new util.HashMap[String, String]()
    val cookieStr = cookie
    val cookieArr = cookieStr.split(";")

    for (line <- cookieArr) {

      val lineArr = line.split("=")
      if (lineArr.length > 1) {

        cookieMap.put(lineArr(0), lineArr(1))

      }
    }

    cookieMap

  }

  /**
   * 爬取KFC用户信息
   * @param ua,cookie 用于爬取用户数据的ua cookie
   * @return 用户数据
   */
  def crawlerBDInfo(ua: String, cookie: String): HashSet[String] = {

    var infoSet = new HashSet[String]
    try {

      val doc = Jsoup.connect("http://waimai.baidu.com/waimai/user/address/select?display=json")
        .ignoreContentType(true)
        .userAgent(ua)
        .cookies(getCookieMap(cookie))
        .timeout(3000)
        .method(Method.GET)
        .execute()
      infoSet = parseHtml(doc.body())


    } catch {

      case ex: IOException =>
        ex.printStackTrace()
        ProxyIP.changIP()
    }

    infoSet
  }

  /**
   * 解析数据
   * @param html 数据str
   * @return 用户数据
   */
  def parseHtml(html: String): HashSet[String] = {

    var infoSet = new HashSet[String]
    var name = ""
    var phone = ""
    var address = ""
    val jo = new JSONObject(html)
    val result = jo.getInt("error_no")

    if (result == 0) {

      val dataArr = jo.getJSONObject("result").optJSONArray("data")

      if (dataArr.length() > 0) {

        for (index <- 0 until dataArr.length()) {

          name = dataArr.get(index).asInstanceOf[JSONObject].getString("user_name")
          phone = dataArr.get(index).asInstanceOf[JSONObject].getString("user_phone")

          val address_detail = dataArr.get(index).asInstanceOf[JSONObject].getString("address")
          val addressComponent = dataArr.get(index).asInstanceOf[JSONObject].getString("component")
          val com = new JSONObject(addressComponent)
          val province = com.getString("province")
          val city = com.getString("city")
          val district = com.getString("district")
          val street = com.getString("street")

          address = province + city + district + street + address_detail
          val info = name + "-->" + phone + "-->" + address
          infoSet = infoSet.+(info)
        }
      }
    }

    infoSet

  }

  /**
   * 获取可爬取数据
   * @param dataFile 数据文件
   * @return 需要的数据ua和cookie
   */
  def getData(dataFile: String): HashSet[(String, String)] = {

    var uaAndCookie = new HashSet[(String, String)]

    for (line <- Source.fromFile(dataFile)("UTF-8").getLines()) {

      val data = line.split("\\t")
      if (data.length == 4) {

        val url = data(0)
        val ref = data(1)
        val ua = data(2)
        val cookie = data(3)

        if ((url + ref).contains("waimai.baidu.com") && cookie != "NoDef") {

          uaAndCookie = uaAndCookie.+((ua, cookie))

        }
      }
    }

    uaAndCookie
  }


  /**
   * 用于保存微博信息
   * @param baiDuInfo：微博用于的信息集合
   */
  def saveBDInfo(baiDuInfo: HashSet[String]): Unit = {

    Class.forName("com.mysql.jdbc.Driver").newInstance()

    val conn = DriverManager.getConnection(AccountInfo.conn_str)
    val statement = conn.createStatement()
    val o2o_ResultSet = statement.executeQuery("SELECT phone,address FROM O2O")

    //获取020中的手机和地址,并保存到集合
    var bdSet = new HashSet[String]

    while (o2o_ResultSet.next()) {

      val phone = o2o_ResultSet.getString("phone")
      val address = o2o_ResultSet.getString("address")
      bdSet = bdSet.+(phone + address)

    }

    val main_phoneResultSet = statement.executeQuery("SELECT phone ,id FROM main_index where phone<> \"\"")

    //获取main_index中的微博id,并保存到map
    val mainPhoneMap = new util.HashMap[String, Int]

    while (main_phoneResultSet.next()) {

      val id = main_phoneResultSet.getInt("id")
      val phone = main_phoneResultSet.getString("phone")
      mainPhoneMap.put(phone, id)

    }

    for (infoStr <- baiDuInfo) {

      val infoArr = infoStr.split("-->")
      val real_name = infoArr(0)
      val phone = infoArr(1)
      val address = infoArr(2)

      //判断主表是否有此手机号，没有则添加进主表
      if (!mainPhoneMap.containsKey(phone)) {

        try {

          val prep = conn.prepareStatement("INSERT INTO main_index (phone,qq,weibo) VALUES (?, ?, ? ) ")
          prep.setString(1, phone)
          prep.setString(2, "")
          prep.setString(3, "")
          prep.executeUpdate

          var main_index_id = 0
          val main_idPhoneResult = statement.executeQuery("SELECT id FROM main_index where phone='" + phone + "'")

          if (main_idPhoneResult.next()) {

            main_index_id = main_idPhoneResult.getInt("id")

          }

          mainPhoneMap.put(phone, main_index_id)
        } catch {

          case ex: SQLException => ex.printStackTrace()

        }
      }


      if (!bdSet.contains(phone + address)) {

        try {

          //将数据写到o2o表中
          val prep = conn.prepareStatement("INSERT INTO O2O (main_index_id, phone,email,real_name,address,platform) VALUES (?, ?, ?, ?, ?, ?) ")
          prep.setInt(1, mainPhoneMap.get(phone))
          prep.setString(2, phone)
          prep.setString(3, "")
          prep.setString(4, real_name)
          prep.setString(5, address)
          prep.setInt(6, 1)
          prep.executeUpdate
        } catch {

          case ex: SQLException => ex.printStackTrace()

        }
      }
    }

    conn.close()

  }
}

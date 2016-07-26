package com.kunyan.userportrait.temp.lcm.crawler.O2O;

import scala.collection.immutable.HashSet;

import java.util.concurrent.Callable;

/**
 * Created by lcm on 2016/5/26.
 *
 */
public class CrawlerBaiDuTask implements Callable<HashSet<String>> {

    private String ua = "";
    private String cookie = "";

    public CrawlerBaiDuTask(String ua, String cookie) {
        this.ua = ua;
        this.cookie = cookie;
    }

    @Override
    public HashSet<String> call() throws Exception {

        Thread.sleep(100);
        return CrawlerBaiDu.crawlerBDInfo(ua, cookie);
    }
}

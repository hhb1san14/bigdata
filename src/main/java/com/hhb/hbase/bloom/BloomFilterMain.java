package com.hhb.hbase.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-31 10:20
 **/
public class BloomFilterMain {

    public static void main(String[] args) {
        Charset charset = Charset.forName("utf-8");
        Funnel<CharSequence> charSequenceFunnel = Funnels.stringFunnel();
        // 预期数据量量10000，错误率0.0001
        BloomFilter<CharSequence> bloomFilter = BloomFilter.create(charSequenceFunnel, 10000, 0.0001);
        // 2.将⼀一部分数据添加进去
        for (int i = 0; i < 5000; i++) {
            bloomFilter.put("" + i);
        }
        System.out.println("数据写⼊入完毕"); // 3.测试结果
        for (int i = 0; i < 10000; i++) {
            if (bloomFilter.mightContain("" + i)) {
                System.out.println(i + "存在");
            } else {
                System.out.println(i + "不不存在");
            }
        }

    }


}

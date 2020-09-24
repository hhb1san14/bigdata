package com.hhb.kafka.deserialize;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-17 20:00
 **/
public class UserDeserializer implements Deserializer<User> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        //分配空间
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        //把byte数据写入到byteBuffer中，只是游标指向最后，级bytes长度的位置
        byteBuffer.put(bytes);
        //将游标指向最开始
        byteBuffer.flip();
        //获取第一个int
        int userId = byteBuffer.getInt();
        // 获取第二个int,即userName的长度
        int length = byteBuffer.getInt();
        // 生成userName
        String userName = new String(bytes, 8, length);

        return new User(userId, userName);
    }

    @Override
    public void close() {

    }
}

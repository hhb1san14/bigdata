package com.hhb.kafka.serialize;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-08-13 20:05
 **/
public class UserSerializer implements Serializer<User> {

    /**
     * 用户接收对序列化器的配置参数，并对当前序列化器进行配置和初始化的
     *
     * @param map
     * @param b
     */
    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //do nothing
    }

    /**
     * 将User的数据转化成字节数组
     *
     * @param s
     * @param user
     * @return
     */
    @Override
    public byte[] serialize(String s, User user) {


        if (user == null) {
            return null;
        }

        Integer userId = user.getUserId();
        String userName = user.getUserName();
        try {
            if (userId != null && userName != null) {
                byte[] bytes = userName.getBytes("UTF-8");
                int length = bytes.length;
                //申请一块内存，存放数据
                //第一个4个字节，用于存储userId的值
                //第二个4个字节，用于存放userName字节数组的长度int值
                //第三个长度，用于存放userName序列化之后的字节数组
                ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + length);
                //设置userId
                byteBuffer.putInt(userId);
                //设置长度
                byteBuffer.putInt(length);
                //设置序列化后的userName
                byteBuffer.put(bytes);
                //返回
                return byteBuffer.array();
            }
        } catch (Exception e) {
            throw new SerializationException("序列化对象：User 异常");
        }
        return null;
    }

    /**
     * 用户关闭资源等操作，需要幂等
     */
    @Override
    public void close() {
        //do nothing
    }
}

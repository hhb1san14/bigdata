package com.hhb.kafka.serialize;

/**
 * @description: 用户自定义封装消息的实体类
 * @author: huanghongbo
 * @date: 2020-08-13 20:01
 **/
public class User {

    private Integer userId;

    private String userName;

    public Integer getUserId() {
        return userId;
    }

    public User setUserId(Integer userId) {
        this.userId = userId;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public User setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    @Override
    public String toString() {
        return "User{" +
                "userId=" + userId +
                ", userName='" + userName + '\'' +
                '}';
    }
}

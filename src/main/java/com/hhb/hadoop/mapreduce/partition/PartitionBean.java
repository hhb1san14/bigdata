package com.hhb.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-05 16:07
 * @Description: 需求：
 * * 按照不同的appkey把记录输出到不同的分区中
 * * <p>
 * * 001 001577c3 kar_890809 120.196.100.99 1116 954 200
 * * 日志id 设备id appkey(合作硬件厂商) 网络ip 自有内容时长(秒) 第三方内 容时长(秒) 网络状态码
 */
public class PartitionBean implements Writable {


    /**
     * 日志ID
     */
    private String id;

    /**
     * 设别ID
     */
    private String deviceId;

    /**
     * appKey
     */
    private String appKey;

    private String ip;

    private String selfDuration;

    private String thirdPartDuration;


    public String getId() {
        return id;
    }

    public PartitionBean setId(String id) {
        this.id = id;
        return this;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public PartitionBean setDeviceId(String deviceId) {
        this.deviceId = deviceId;
        return this;
    }

    public String getAppKey() {
        return appKey;
    }

    public PartitionBean setAppKey(String appKey) {
        this.appKey = appKey;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public PartitionBean setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public String getSelfDuration() {
        return selfDuration;
    }

    public PartitionBean setSelfDuration(String selfDuration) {
        this.selfDuration = selfDuration;
        return this;
    }

    public String getThirdPartDuration() {
        return thirdPartDuration;
    }

    public PartitionBean setThirdPartDuration(String thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
        return this;
    }

    @Override
    public String toString() {
        return id + "\t" + deviceId + "\t" + appKey + "\t" + ip + "\t" + selfDuration + "\t" + thirdPartDuration;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(id);
        output.writeUTF(deviceId);
        output.writeUTF(appKey);
        output.writeUTF(ip);
        output.writeUTF(selfDuration);
        output.writeUTF(thirdPartDuration);

    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.id = input.readUTF();
        this.deviceId = input.readUTF();
        this.appKey = input.readUTF();
        this.ip = input.readUTF();
        this.selfDuration = input.readUTF();
        this.thirdPartDuration = input.readUTF();
    }
}

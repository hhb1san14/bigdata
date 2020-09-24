package com.hhb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @description:
 * @author: huanghongbo
 * @date: 2020-07-29 10:16
 **/
public class HBaseClient {

    private Configuration conf = null;

    private Connection conn = null;

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux121,linux122,linux123");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conn = ConnectionFactory.createConnection(conf);
    }


    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        //获取HBaseAdmin操作表
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        //创建表描述器
        HTableDescriptor person = new HTableDescriptor(TableName.valueOf("person"));
        //增加列族
        person.addFamily(new HColumnDescriptor("info"));
        person.addFamily(new HColumnDescriptor("address"));
        admin.createTable(person);
        admin.close();
        System.err.println("创建表成功");
    }


    /**
     * 插入数据
     *
     * @throws IOException
     */
    @Test
    public void putData() throws IOException {
        HTable person = (HTable) conn.getTable(TableName.valueOf("person"));
        List<Put> list = new ArrayList<>();
        //rowKey
        Put put1 = new Put(Bytes.toBytes("110"));
        //分别设置：列族、列、值
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
        list.add(put1);

        Put put2 = new Put(Bytes.toBytes("110"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        list.add(put2);

        Put put3 = new Put(Bytes.toBytes("110"));
        put3.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
        list.add(put3);
        person.put(list);

        person.close();
        System.err.println("插入数据成功");
    }


    /**
     * 查询数据
     *
     * @throws IOException
     */
    @Test
    public void selectData() throws IOException {
        HTable person = (HTable) conn.getTable(TableName.valueOf("person"));
        // rowKey
        Get get = new Get(Bytes.toBytes("110"));
//        get.addFamily(Bytes.toBytes("info"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//        get.addColumn()
        Result result = person.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            System.err.println("rowKey信息：  " + Bytes.toString(CellUtil.cloneRow(cell)));
            System.err.println("列族信息：  " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.err.println("列信息：  " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.err.println("value信息：  " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        person.close();
    }

    /**
     * 扫描全表数据
     *
     * @throws IOException
     */
    @Test
    public void scanData() throws IOException {
        HTable person = (HTable) conn.getTable(TableName.valueOf("person"));
        Scan scan = new Scan();
        ResultScanner scanner = person.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.err.println("rowKey信息：  " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.err.println("列族信息：  " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.err.println("列信息：  " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.err.println("value信息：  " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


    /**
     * 扫描全表数据的部分数据
     *
     * @throws IOException
     */
    @Test
    public void scanStartEndData() throws IOException {
        HTable person = (HTable) conn.getTable(TableName.valueOf("person"));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("110"));
        scan.setStopRow(Bytes.toBytes("111"));
        ResultScanner scanner = person.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.err.println("rowKey信息：  " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.err.println("列族信息：  " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.err.println("列信息：  " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.err.println("value信息：  " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        person.close();
    }

    /**
     * 删除数据
     */
    @Test
    public void deleteData() throws IOException {
        HTable person = (HTable) conn.getTable(TableName.valueOf("person"));
        Delete delete = new Delete(Bytes.toBytes("110"));
        delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        person.delete(delete);

        person.close();
    }


    @After
    public void close() throws IOException {
        if (conn != null) {
            conn.close();
        }
    }


}

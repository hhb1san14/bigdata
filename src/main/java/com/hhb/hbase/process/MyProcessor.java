package com.hhb.hbase.process;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableWrapper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @description:需求： 通过协处理器Observer实现Hbase当中t1表插入数据, 指定的另一张表t2也需要插入相对应的数据。
 * 正常需要实现RegionObserver接口，但是该接口RegionObserver有默认的实现类BaseRegionObserver
 * 所以只需要继承BaseRegionObserver类，实现prePut方法即可，这样就可以监听的t1表插入数据时，执行向t2表插入数据
 * @author: huanghongbo
 * @date: 2020-07-29 19:29
 **/

public class MyProcessor extends BaseRegionObserver {

    private static final Logger logger = LoggerFactory.getLogger(MyProcessor.class);

    /**
     * 把自己执行逻辑定义在这里
     *
     * @param e          上下文,通过上下文获取环境变量等信息
     * @param put        向t1表插入数据的对象，如果向t2插入，与t1的put一样就好
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取t2表的table对象
        HTableWrapper t2 = (HTableWrapper) e.getEnvironment().getTable(TableName.valueOf("t2"));
        //table对象.put
        Put t2Put = new Put(put.getRow());
        //解析数据
        List<Cell> cells = put.get(Bytes.toBytes("info"), Bytes.toBytes("name"));
        //获取最新的数据
        Cell cell = cells.get(0);
        t2Put.add(cell);
        t2.put(t2Put);
        t2.close();
    }
//
//    @Override
//    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
//        HTableWrapper t2 = (HTableWrapper) e.getEnvironment().getTable(TableName.valueOf("t2"));
//        //要删除的rowkey，也是本次的要删除的列，根据要删除的row找出要删除的列
//        byte[] row = delete.getRow();
//        logger.info("删除的rowkey为：" + Bytes.toString(row));
//        //获取没个列族对应的cell信息
//        NavigableMap<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
//        Set<Map.Entry<byte[], List<Cell>>> entries = familyCellMap.entrySet();
//        for (Map.Entry<byte[], List<Cell>> entry : entries) {
//            List<Cell> cells = entry.getValue();
//            for (Cell cell : cells) {
//                byte[] rowKey = CellUtil.cloneRow(cell); //rowKey
//                byte[] family = CellUtil.cloneFamily(cell); //列族
//                byte[] column = CellUtil.cloneQualifier(cell); // 列信息
//                logger.info("遍历：rowkey: " + Bytes.toString(rowKey) + "列族： " + Bytes.toString(family) + " 列信息： " + Bytes.toString(column));
//            }
//        }
//        super.postDelete(e, delete, edit, durability);
//    }
}

package com.example.test.hbase.service;

import java.io.IOException;
import java.util.List;

/**
 * Created by fantastyJ on 2018/12/13 3:54 PM
 */
public interface HBaseService {

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param family    列簇
     */
    void createTable(String tableName, String[] family);

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    void deleteTable(String tableName);

    /**
     * 插入一行
     *
     * @param tableName  表名
     * @param rowKey     行健
     * @param familyName 列簇
     * @param columnName 列名
     * @param value      值
     * @throws IOException
     */
    void insertRow(String tableName, String rowKey,
                   String familyName, String columnName, String value) throws IOException;

    /**
     * 根据rowkey 获取某一行的值
     *
     * @param tableName 表名
     * @param rowkey    行键
     * @param family    列簇
     * @param column    列名
     * @return
     */
    String getValue(String tableName, String rowkey, String family, String column);

    /**
     * 根据起始结束的rowKey来查询结果
     *
     * @param tableName 表名
     * @param family    列簇
     * @param column    列名
     * @param startRow  起始key
     * @param stopRow   结束key
     * @return
     */
    List<String> getValueByStartStopRowKey(String tableName, String family, String column, String startRow, String stopRow);


    /**
     * 删除某一行的某一列
     *
     * @param tableName  表名
     * @param rowKey     行键
     * @param falilyName 列簇
     * @param columnName 列名
     * @throws IOException
     */
    void deleteColumn(String tableName, String rowKey,
                      String falilyName, String columnName) throws IOException;


    /**
     * 删除某一行
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @throws IOException
     */
    void deleteAllColumn(String tableName, String rowKey) throws IOException;
}


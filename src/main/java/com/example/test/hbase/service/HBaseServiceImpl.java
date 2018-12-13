package com.example.test.hbase.service;

import com.example.test.hbase.config.HBaseConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by fantastyJ on 2018/12/13 3:54 PM
 */
@Service
public class HBaseServiceImpl implements HBaseService {

    @Autowired
    private HBaseConfig con;

    @Override
    public void createTable(String tableName, String[] family) {
        Connection connection = null;
        if (StringUtils.isBlank(tableName) || family == null || family.length == 0) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            Admin admin = connection.getAdmin();
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < family.length; i++) {
                desc.addFamily(new HColumnDescriptor(family[i]));
            }
            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("table Exists!");
                System.exit(0);
            } else {
                admin.createTable(desc);
                System.out.println("create table Success!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteTable(String tableName) {
        Connection connection = null;
        if (StringUtils.isBlank(tableName)) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println(tableName + " is deleted!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据tableName，rowkey，family coluem 查询单个数据
     */
    public String getValue(String tableName, String rowkey, String family, String column) {
        Table table = null;
        Connection connection = null;
        String res = "";
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(rowkey.getBytes());
            g.addColumn(family.getBytes(), column.getBytes());
            Result result = table.get(g);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    res = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }


    public List<String> getValueByStartStopRowKey(String tableName, String family, String column, String startRow, String stopRow) {
        Table table = null;
        Connection connection = null;
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(startRow) || StringUtils.isBlank(stopRow)
                || StringUtils.isBlank(column)) {
            return null;
        }
        List<String> rs = new ArrayList<>();
        try {
            connection = ConnectionFactory.createConnection(con.configuration());
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner result = table.getScanner(scan);
            result.forEach(r -> {
                Map map = r.getFamilyMap(Bytes.toBytes(family));
                List<Cell> cells = r.listCells();
                cells.forEach(c -> rs.add(Bytes.toString(CellUtil.cloneRow(c)) + ":::" + Bytes.toString(CellUtil.cloneValue(c))));
            });
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rs;
    }

    @Override
    public void insertRow(String tableName, String rowKey,
                                   String familyName, String columnName, String value) {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)
                || StringUtils.isBlank(familyName) || StringUtils.isBlank(columnName)
                || StringUtils.isBlank(value)) {
            return;
        }
        try {
            Connection connection = ConnectionFactory.createConnection(con.configuration());
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                    Bytes.toBytes(value));
            table.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("update table Success!");
    }

    public void deleteColumn(String tableName, String rowKey,
                                    String falilyName, String columnName){
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)
                || StringUtils.isBlank(falilyName) || StringUtils.isBlank(columnName)) {
            return;
        }
        try {
            Connection connection = ConnectionFactory.createConnection(con.configuration());

            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
            deleteColumn.addColumn(Bytes.toBytes(falilyName),
                    Bytes.toBytes(columnName));
            table.delete(deleteColumn);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    public void deleteAllColumn(String tableName, String rowKey){

        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)) {
            return;
        }
        try {
            Connection connection = ConnectionFactory.createConnection(con.configuration());

            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
            table.delete(deleteAll);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("all columns are deleted!");
    }

}


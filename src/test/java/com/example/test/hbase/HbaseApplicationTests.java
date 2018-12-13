package com.example.test.hbase;

import com.example.test.hbase.service.HBaseService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseApplicationTests {

    @Autowired
    private HBaseService hbaseServiceImpl;

    @Test
    public void getData(){
        String result = hbaseServiceImpl.getValue("test","test","t","val");
        System.out.println(result);
    }


    @Test
    public void getDataByStartStopRow(){
        List<String> result = hbaseServiceImpl.getValueByStartStopRowKey("test_real_data","gprs","gprs","d-1040-20181130-20181201033211","d-1040-20181130-20181201035211");
        System.out.println(result);
    }

    @Test
    public void createTable(){
        hbaseServiceImpl.createTable("test",new String[]{"t"});
    }

    @Test
    public void deleteTable(){
        hbaseServiceImpl.deleteTable("test");
    }

    @Test
    public void insertData() throws Exception{
        hbaseServiceImpl.insertRow("test","test","t","val","this is just a test");
    }

    @Test
    public void deleteColumn() throws Exception{
        hbaseServiceImpl.deleteColumn("test","test","t","val");
    }

    @Test
    public void deleteRow() throws Exception{
        hbaseServiceImpl.deleteAllColumn("test","test");
    }

}


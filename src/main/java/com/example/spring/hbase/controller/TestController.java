package com.example.spring.hbase.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Created by sunboyu on 2018/2/1.
 */
@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @GetMapping
    public String test() {
        Scan scan = new Scan();
        hbaseTemplate.find("emp", scan, new RowMapper<Result>() {
            @Override
            public Result mapRow(Result result, int i) throws Exception {
                return result;
            }
        });
        return "done";
    }

    @GetMapping("put")
    public String put() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.124.128");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) conn.getTable(TableName.valueOf("member"));

        Put put = new Put("zpc".getBytes());

        put.addColumn("info".getBytes(), "age".getBytes(), "20".getBytes());

        table.put(put);
        conn.close();
        return "done";
    }

    @GetMapping("/1")
    public String test1() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.124.128");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(conf);

        Scan scan = new Scan();

        HTable table = (HTable) conn.getTable(TableName.valueOf("emp"));

        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {

            for (Cell cell : result.rawCells()) {
                System.out.println("row:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("timestamp:" + cell.getTimestamp());
                System.out.println("-------------------------------------------");
            }
        }
        conn.close();
        return null;
    }
}

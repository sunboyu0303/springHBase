package com.example.spring.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseJavaAPI {

    // 声明静态配置
    public static Configuration conf = null;
    public static Connection conn = null;

    static {
        System.setProperty("hadoop.home.dir", "D:\\Program Files (x86)\\hadoop-2.6.5");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.124.128");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 判断表是否存在
    public static boolean isExist(String tableName) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        return admin.tableExists(tableName);
    }

    // 创建数据库表
    public static void createTable(String tableName, String[] columnFamilys) throws IOException {

        if (isExist(tableName)) {
            System.out.println("表 " + tableName + " 已存在！");
        } else {

            // 新建一个数据库管理员
            HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();

            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));

            for (String cFamily : columnFamilys) {
                desc.addFamily(new HColumnDescriptor(cFamily));
            }

            admin.createTable(desc);

            System.out.println("创建表 " + tableName + " 成功!");
        }
    }

    // 删除数据库表
    public static void deleteTable(String tableName) throws IOException {

        if (isExist(tableName)) {

            HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
            admin.disableTable(tableName);
            admin.deleteTable(tableName);

            System.out.println("删除表 " + tableName + " 成功！");

        } else {
            System.out.println("删除的表 " + tableName + " 不存在！");
        }
    }

    // 添加一条数据
    public static void addRow(String tableName, String row, String columnFamily, String column, String value)
            throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(row.getBytes());

        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());

        table.put(put);
    }

    // 删除一条(行)数据
    public static void deleteRow(String tableName, String row) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));

        table.delete(new Delete(row.getBytes()));
    }

    // 删除多条数据
    public static void delMultiRows(String tableName, String[] rows) throws Exception {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        List<Delete> delList = new ArrayList<Delete>();

        for (String row : rows) {
            Delete del = new Delete(row.getBytes());
            delList.add(del);
        }
        table.delete(delList);
    }

    // 获取一条数据
    public static void getRow(String tableName, String row) throws Exception {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(row.getBytes());

        Result rs = table.get(get);

        // 输出结果,raw方法返回所有keyvalue数组
        for (Cell cell : rs.rawCells()) {

            System.out.print("行名:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.print("时间戳:" + cell.getTimestamp() + " ");
            System.out.print("列族名:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.print("列名:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("值:" + new String(CellUtil.cloneValue(cell)));
        }
    }

    // 获取所有数据
    public static void getAllRows(String tableName) throws Exception {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);

        for (Result rs : results) {
            for (Cell cell : rs.rawCells()) {

                System.out.print("行名:" + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.print("时间戳:" + cell.getTimestamp() + " ");
                System.out.print("列族名:" + new String(CellUtil.cloneFamily(cell)) + " ");
                System.out.print("列名:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                System.out.println("值:" + new String(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void main(String[] args) {

        try {

            String tableName = "student";
            // 第一步：创建数据库表：“student”
            String[] columnFamilys = {"info", "course"};
            HBaseJavaAPI.createTable(tableName, columnFamilys);

            // 第二步：向数据表的添加数据
            // 添加第一行数据
            if (isExist(tableName)) {

                HBaseJavaAPI.addRow(tableName, "zpc", "info", "age", "20");
                HBaseJavaAPI.addRow(tableName, "zpc", "info", "sex", "boy");
                HBaseJavaAPI.addRow(tableName, "zpc", "course", "china", "97");
                HBaseJavaAPI.addRow(tableName, "zpc", "course", "math", "128");
                HBaseJavaAPI.addRow(tableName, "zpc", "course", "english", "85");
                // 添加第二行数据
                HBaseJavaAPI.addRow(tableName, "henjun", "info", "age", "19");
                HBaseJavaAPI.addRow(tableName, "henjun", "info", "sex", "boy");
                HBaseJavaAPI.addRow(tableName, "henjun", "course", "china", "90");
                HBaseJavaAPI.addRow(tableName, "henjun", "course", "math", "120");
                HBaseJavaAPI.addRow(tableName, "henjun", "course", "english", "90");
                // 添加第三行数据
                HBaseJavaAPI.addRow(tableName, "niaopeng", "info", "age", "18");
                HBaseJavaAPI.addRow(tableName, "niaopeng", "info", "sex", "girl");
                HBaseJavaAPI.addRow(tableName, "niaopeng", "course", "china", "100");
                HBaseJavaAPI.addRow(tableName, "niaopeng", "course", "math", "100");
                HBaseJavaAPI.addRow(tableName, "niaopeng", "course", "english", "99");

                // 第三步：获取一条数据
                System.out.println("**************获取一条(zpc)数据*************");
                HBaseJavaAPI.getRow(tableName, "zpc");

                // 第四步：获取所有数据
                System.out.println("**************获取所有数据***************");
                HBaseJavaAPI.getAllRows(tableName);

                // 第五步：删除一条数据
                System.out.println("************删除一条(zpc)数据************");
                HBaseJavaAPI.deleteRow(tableName, "zpc");
                HBaseJavaAPI.getAllRows(tableName);

                // 第六步：删除多条数据
                System.out.println("**************删除多条数据***************");
                String rows[] = new String[]{"qingqing", "xiaoxue"};
                HBaseJavaAPI.delMultiRows(tableName, rows);
                HBaseJavaAPI.getAllRows(tableName);

                // 第七步：删除数据库
                System.out.println("***************删除数据库表**************");
                HBaseJavaAPI.deleteTable(tableName);

                System.out.println("表" + tableName + "存在吗？" + isExist(tableName));

            } else {
                System.out.println(tableName + "此数据库表不存在！");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

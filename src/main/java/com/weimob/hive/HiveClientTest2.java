package com.weimob.hive;

import org.apache.hadoop.security.UserGroupInformation;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Created by dell on 2018/11/8.
 */
public class HiveClientTest2 {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://sh-ops-hadoop-01-online-10:10000/default;principal=hive/hadoop-hiveserver2-lb-01@WEIMOB.COM";
    private static String sql = "";
    private static ResultSet res;

    public static Connection get_conn() throws SQLException, ClassNotFoundException {
        /** 使用Hadoop安全登录 **/
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");

        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            // 默认：这里不设置的话，win默认会到 C盘下读取krb5.init
            System.setProperty("java.security.krb5.conf", "D:/hive.keytab");
        } // linux 会默认到 /etc/krb5.conf 中读取krb5.conf,本文笔者已将该文件放到/etc/目录下，因而这里便不用再设置了
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("hive/hadoop-hiveserver2-lb-01@WEIMOB.COM", "/root/hive.keytab");
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    public static boolean show_tables(Statement statement) {
        sql = "show databases";
        System.out.println("Running:" + sql);
        try {
            ResultSet res = statement.executeQuery(sql);
            System.out.println("执行“+sql+运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {

        try {
            Connection conn = get_conn();
            Statement stmt = conn.createStatement();
            // 创建的表名
            String tableName = "test01";
            show_tables(stmt);
            // describ_table(stmt, tableName);
            /** 删除表 **/
            // drop_table(stmt, tableName);
            // show_tables(stmt);
            // queryData(stmt, tableName);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("!!!!!!END!!!!!!!!");
        }
    }

}

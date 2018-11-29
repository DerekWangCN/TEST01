package com.weimob.jdbc;

import org.apache.phoenix.query.QueryServices;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;


/**
 * Created by dell on 2018/11/3.
 */
public class PhoenixInput {
    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Statement stmt = null;
        ResultSet rs = null;

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"100000000"); //改变默认的500000
        connectionProperties.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"100000000"); // 默认是500000
        Connection con = DriverManager.getConnection("jdbc:phoenix:sh-ops-hadoop-01-online-12:2181",connectionProperties);
    //    Connection con = DriverManager.getConnection("jdbc:phoenix:sh-ops-hadoop-01-online-13:2181");
        stmt = con.createStatement();
        //UPSERT INTO "TEST01" VALUES('1','aaaaaa',111111);
        //upsert into TEST01 (ID,A_COL) values ('1','aa');
        int startLine = Integer.parseInt(args[0]);
        int endLine = Integer.parseInt(args[1]);
        String tableName = args[2];
        System.out.println("startLine = " + startLine);
        System.out.println("endLine = " + endLine);
        int count = 0;
        long begin=System.currentTimeMillis();
        for (int i = startLine ; i <= endLine ; i++) {
            UUID uuid = UUID.randomUUID();
            String sql = "UPSERT INTO \"" + tableName + "\" (ID,A_COL) VALUES('" + i + "','" + String.valueOf(uuid) + "')";
//            System.out.println(sql);
            stmt = con.prepareStatement(sql);
            stmt.execute(sql);
            count++;
        }
//          while (rs.next()) {
//              System.out.print("ID:"+rs.getString("ID"));
//              System.out.println(",A_COL:"+rs.getString("A_COL"));
//          }
        long begin2=System.currentTimeMillis();
        con.commit();
        stmt.close();
        con.close();
        long end=System.currentTimeMillis();
        long end2=System.currentTimeMillis();
        System.out.println("PhoenixTime:" + "Count=" + count + "  time=" + (end - begin));
        System.out.println("PhoenixTime2:" + "Count=" + count + "  time2=" + (end2 - begin2));
    }
}

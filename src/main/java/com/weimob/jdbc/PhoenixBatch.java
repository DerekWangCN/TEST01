package com.weimob.jdbc;

import org.apache.phoenix.query.QueryServices;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;


/**
 * Created by dell on 2018/11/3.
 */
public class PhoenixBatch {
    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static void main(String[] args) throws SQLException {

        int time = Integer.parseInt(args[0]);
        int numInterval = Integer.parseInt(args[1]);
//        int time = 2;
//        int numInterval = 1;

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;

        Connection con = DriverManager.getConnection("jdbc:phoenix:sh-ops-hadoop-01-online-12:2181");
        con.setAutoCommit(false);
        stmt = con.prepareStatement("UPSERT INTO PHOENIXOUT01 VALUES(?,?,?)");
        int count = 0;
        long begin=System.currentTimeMillis();
        for (int i = 1 ; i <= time ; i++) {
            UUID uuid = UUID.randomUUID();
            Timestamp d = new Timestamp(System.currentTimeMillis());
            String string = uuid.toString() + d.toString();
            stmt.setString(1, String.valueOf(i));
            stmt.setString(2, String.valueOf(string));
            stmt.setString(3, String.valueOf(i));
            stmt.addBatch();
            if (i % numInterval == 0) {
                stmt.executeBatch();
                con.commit();
                count++;
                System.out.println("count = " + count);
            }
        }
        stmt.executeBatch();
        con.commit();
        stmt.close();
        con.close();
        long end=System.currentTimeMillis();
        System.out.println("PhoenixTime:" + "Count=" + count + "  time=" + (end - begin));
    }
}

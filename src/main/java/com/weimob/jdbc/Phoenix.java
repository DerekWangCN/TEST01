package com.weimob.jdbc;

import org.apache.phoenix.query.QueryServices;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;


/**
 * Created by dell on 2018/11/3.
 */
public class Phoenix {
    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"100000000"); //改变默认的500000
        connectionProperties.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB, "100000000"); // 默认是500000
        Connection con = DriverManager.getConnection("jdbc:phoenix:sh-ops-hadoop-01-online-12:2181");
        stmt = con.prepareStatement("UPSERT INTO TEST03 VALUES(?,?,?)");
        //UPSERT INTO "TEST01" VALUES('1','aaaaaa',111111);
        int count = 0;
        long begin=System.currentTimeMillis();
        for (int i = 1 ; i <= 50000 ; i++) {
            stmt.clearBatch();
            UUID uuid = UUID.randomUUID();
            Timestamp d = new Timestamp(System.currentTimeMillis());
            String sql = "UPSERT INTO \"TEST03\" VALUES('" + uuid + "','" + uuid + d + "',"+ i +")";
              stmt = con.prepareStatement(sql);
              stmt.execute(sql);
              count++;
        }
//          while (rs.next()) {
//              System.out.print("ID:"+rs.getString("ID"));
//              System.out.println(",A_COL:"+rs.getString("A_COL"));
//          }
        con.commit();
        stmt.close();
        con.close();
        long end=System.currentTimeMillis();
        System.out.println("PhoenixTime:" + "Count=" + count + "  time=" + (end - begin));
    }
}

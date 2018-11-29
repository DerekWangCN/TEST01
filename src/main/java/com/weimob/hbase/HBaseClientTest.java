package com.weimob.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.Timestamp;
import java.util.UUID;

/**
 * Created by dell on 2018/11/3.
 */
public class HBaseClientTest {
    public static HTable getTable(String name) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf, name);

        return table;
    }

    /**
     * get 'tb_name','row','cf:col'
     *
     * @param table
     * @throws Exception
     */
    public static void getData(HTable table) throws Exception {

        Get get = new Get(Bytes.toBytes("20181103_10001"));
        // get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.addFamily(Bytes.toBytes("info"));

        Result rs = table.get(get);

        for (Cell cell : rs.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + "-->" + Bytes.toString(CellUtil.cloneQualifier(cell))
                            + "-->" + Bytes.toString(CellUtil.cloneValue(cell)) + "-->" + cell.getTimestamp());
            System.out.println("---------------------------------------------------------------------");
        }
        table.get(get);
    }

    public static void putData(HTable table) throws Exception {

          int count = 0;
          long begin=System.currentTimeMillis();
          Put put ;
          for(int i = 1 ; i <= 20000 ; i++) {
//              Put put = new Put(Bytes.toBytes("" + i));
              put = new Put(Bytes.toBytes("" + i));
              put.add(Bytes.toBytes("0"), Bytes.toBytes("A_COL"), Bytes.toBytes("bbb"));
              table.put(put);
              count++;
          }
          table.flushCommits();
          long end=System.currentTimeMillis();
          System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
       // getData(table);
    }

    public static void putData2(HTable table) throws Exception {

        int count = 0;
        long begin=System.currentTimeMillis();
        Timestamp d = new Timestamp(System.currentTimeMillis());
        Put put ;
        for(int i = 1 ; i <= 2 ; i++) {
            UUID uuid = UUID.randomUUID();
            put = new Put(Bytes.toBytes(String.valueOf(uuid)));
            put.add(Bytes.toBytes("0"), Bytes.toBytes("A_COL"), Bytes.toBytes(String.valueOf(uuid)+d));
            put.add(Bytes.toBytes("0"), Bytes.toBytes("B_COL"), Bytes.toBytes(i));
            table.put(put);

            count++;
        }

        table.flushCommits();
        long end=System.currentTimeMillis();
        System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
        // getData(table);
    }

    public static void deleteData(HTable table) throws Exception {

        Delete del = new Delete(Bytes.toBytes("20181103_10001"));
        del.deleteColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));

        table.delete(del);

        getData(table);
    }

    public static void scanData(HTable table) throws Exception {

        Scan scan = new Scan();

        ResultScanner rsscan = table.getScanner(scan);
        for (Result rs : rsscan) {
            System.out.println(Bytes.toString(rs.getRow()));
            for (Cell cell : rs.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "-->"
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-->"
                        + Bytes.toString(CellUtil.cloneValue(cell)) + "-->" + cell.getTimestamp());
            }
            System.out.println("---------------------------------------------------------------------");
        }
    }


    public static void rangData(HTable table) throws Exception {
        int count=0;
        Scan scan = new Scan();

        // scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        scan.setStartRow(Bytes.toBytes("2000000"));
        scan.setStopRow(Bytes.toBytes("2010000"));


        ResultScanner rsscan = table.getScanner(scan);
        long begin=System.currentTimeMillis();
        for (Result rs : rsscan) {
            System.out.println(Bytes.toString(rs.getRow()));
            for (Cell cell : rs.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "-->"
                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-->"
                        + Bytes.toString(CellUtil.cloneValue(cell)) + "-->" + cell.getTimestamp());
            }
            count++;
            System.out.println("---------------------------------------------------------------------");
        }
        long end=System.currentTimeMillis();
        table.flushCommits();
        System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("==================HBASE START=======================");
        HTable table = getTable("TEST03");
        // getData(table);
         putData2(table);
        // deleteData(table);
       //  scanData(table);
        // rangData(table);
        System.out.println("==================HBASE END=======================");
    }
}

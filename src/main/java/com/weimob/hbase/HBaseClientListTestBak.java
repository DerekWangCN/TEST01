package com.weimob.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Created by dell on 2018/11/3.
 */
public class HBaseClientListTestBak {
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

    public static void putData(HTable table , int line , int num) throws Exception {
          System.out.println("==============putData====================");
          int count = 0;
          List<Put> puts= new ArrayList<Put>();

          long begin=System.currentTimeMillis();
          Put put ;
          Random random = new Random();
          for(int i = 1 ; i <= line ; i++) {
              UUID uuid = UUID.randomUUID();
              Timestamp d = new Timestamp(System.currentTimeMillis());
              String str1 = uuid.toString() + d.toString();
              int ran1 = random.nextInt(10000);
              put = new Put(Bytes.toBytes(String.valueOf(i)));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("A_COL"), Bytes.toBytes(str1));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("B_COL"), Bytes.toBytes(String.valueOf(i)));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("C_COL"), Bytes.toBytes(str1));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("D_COL"), Bytes.toBytes(i));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("E_COL"), Bytes.toBytes(i));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("F_COL"), Bytes.toBytes(ran1));
//              put.add(Bytes.toBytes("0"), Bytes.toBytes("G_COL"), Bytes.toBytes(ran1));
              put.add(Bytes.toBytes("0"), Bytes.toBytes("A_COL"), Bytes.toBytes(str1));
              put.add(Bytes.toBytes("0"), Bytes.toBytes("B_COL"), Bytes.toBytes(String.valueOf(i)));
              puts.add(put);
              count++;
              if (i % num == 0) {
                  table.put(puts);
                  puts.clear();
                  System.out.println("count = " + count);
              }
          }
          table.flushCommits();
          long end=System.currentTimeMillis();
          puts.clear();
          System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
    }

    public static void putData2(String tableName , int line) throws IOException {
        System.out.println("==============putData2====================");
        Configuration conf = HBaseConfiguration.create();
        Connection conn;
        conn = ConnectionFactory.createConnection(conf);

        long begin=System.currentTimeMillis();
        List<Put> puts= new ArrayList<Put>();
        Put put;
        int count = 0;
        for(int i = 1 ; i <= line ; i++) {
            put = new Put(Bytes.toBytes("" + i));
            UUID uuid = UUID.randomUUID();
            put.add(Bytes.toBytes("0"), Bytes.toBytes("A_COL"), Bytes.toBytes(String.valueOf(uuid)));
            puts.add(put);
            count++;
        }
        BufferedMutator mutator = null;
        TableName tn = TableName.valueOf(tableName);
        BufferedMutatorParams params = new BufferedMutatorParams(tn);
        params.writeBufferSize(5 * 1024 * 1024);

        mutator = conn.getBufferedMutator(params);
        long begin2=System.currentTimeMillis();
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
        }

        long end=System.currentTimeMillis();
        long end2=System.currentTimeMillis();
        System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
        System.out.println("Time2:" + "  Count=" + count + "   time=" + (end2 - begin2));
    }

    public static void deleteData(HTable table , int startRow , int endRow) throws Exception {
        List<Delete> deletes= new ArrayList<Delete>();

        long begin=System.currentTimeMillis();
        int count = 0;

        Delete del;
        for (int i = startRow ; i < endRow ; i++) {
            del = new Delete(Bytes.toBytes(String.valueOf(i)));
            del.deleteColumn(Bytes.toBytes("0"), Bytes.toBytes("A_COL"));
            del.deleteColumn(Bytes.toBytes("0"), Bytes.toBytes("B_COL"));

            deletes.add(del);
            count++;
            if (i % 10000 == 0) {
                table.delete(deletes);
                deletes.clear();
                System.out.println("count = " + count);
            }
        }
        table.flushCommits();
        long end=System.currentTimeMillis();
        deletes.clear();
        System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
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


    public static void rangData(HTable table , String startKey , String endKey) throws Exception {
        int count=0;
        Scan scan = new Scan();

        // scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(endKey));


        ResultScanner rsscan = table.getScanner(scan);
        long begin=System.currentTimeMillis();
        for (Result rs : rsscan) {
//            for (Cell cell : rs.rawCells()) {
//                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "-->"
//                        + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-->"
//                        + Bytes.toString(CellUtil.cloneValue(cell)) + "-->" + cell.getTimestamp()
//                        + CellUtil.cloneValue(cell) + "-->" + cell.getTimestamp());
//            }
            count++;
            if(count % 10000 == 0) {
                System.out.println("count = " + count + " rowkey = "  + Bytes.toString(rs.getRow()));
            }
//            System.out.println("---------------------------------------------------------------------");
        }
        long end=System.currentTimeMillis();
        table.flushCommits();
        System.out.println("HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("==================HBASE START=======================");

//          String key = args[0];
//          String tableName = args[1];
//          int line = Integer.parseInt(args[2]);
//          int numInterVal = Integer.parseInt(args[3]);

          //rowkey
//          String startKey = args[2];
//          String endKey = args[3];

          //delete
            String key = args[0];
            String tableName = args[1];
            int startRow  = Integer.valueOf(args[2]);
            int endRow = Integer.valueOf(args[3]);

//            String key = "3";
//            String tableName = "PHOENIXOUT01";
//            int startRow  = 10000;
//            int endRow = 100001;


//            String key = "3";
//            String tableName = "PHOENIXOUT01";
//            int line = Integer.parseInt("2");
//            int numInterVal = Integer.parseInt("1");


          System.out.println("key = " + key);
          System.out.println("tableName = " + tableName);
//          System.out.println("line = " + line);
//          System.out.println("numInterVal = " + numInterVal);
          HTable table = getTable(tableName);
//        // getData(table);
//        // putData(table);
//        // deleteData(table);
//       //  scanData(table);
//          rangData(table , startKey , endKey);
        if (key.equalsIgnoreCase("1")) {
//            putData(table , line ,numInterVal);
        } else if (key.equalsIgnoreCase("2")) {
//            putData2(tableName , line);
        } else if (key.equalsIgnoreCase("3")) {
            deleteData(table,startRow,endRow);
        } else {
            System.out.println("please input again!!");
        }

        System.out.println("==================HBASE END=======================");
    }
}

package com.weimob.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by dell on 2018/11/3.
 */
public class HBaseClientThreadTest extends Thread {
    private String tableName;
    private int startRow;
    private int endRow;

    public HBaseClientThreadTest(String tableName , int startRow , int endRow ) {
        this.tableName = tableName;
        this.startRow = startRow;
        this.endRow = endRow;
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

    public void run() {
        String threadName = "===Thread:[ " + startRow + " - " + endRow + " ]===";
        System.out.println(threadName + " Start");
        Configuration conf = HBaseConfiguration.create();
        HTable table = null;

        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        int count = 0;
        List<Put> puts= new ArrayList<Put>();

        long begin=System.currentTimeMillis();

        Put put ;
        for(int i = startRow ; i <= endRow ; i++) {
            put = new Put(Bytes.toBytes("" + i));
            UUID uuid = UUID.randomUUID();
            put.add(Bytes.toBytes("0"), Bytes.toBytes("A_COL"), Bytes.toBytes(String.valueOf(uuid)));
            puts.add(put);
            count++;
        }
        System.out.println("====" + threadName +"  put start");
        long begin2=System.currentTimeMillis();
        try {
            table.put(puts);
            table.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        }
        long end=System.currentTimeMillis();
        long end2=System.currentTimeMillis();
        puts.clear();
        System.out.println(threadName + " HBaseTime:" + "Count=" + count + "  time=" + (end - begin));
        System.out.println(threadName + " Time2:" + "  Count=" + count + "   time=" + (end2 - begin2));
        System.out.println(threadName + " End");
    }
}

package com.weimob.utils

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


/**
 * Created by wanghb on 2018/11/7.
 */
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static Configuration conf;
    private static Connection conn;

    /**
     * 获得zk配置信息
     * @param zkHost
     */
    public static void init(String zkHost) {
        if (conf == null) {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkHost);
            conf.set("zookeeper.znode.parent", "/hbase");
        }
    }

    /**
     * 获得链接
     * @return
     */
    public static synchronized Connection getConnection() {
        if(conn == null || conn.isClosed()) {
            conn = ConnectionFactory.createConnection(conf);
        }

        return conn;
    }

    public static long putData(String tablename , List<Put> puts) {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    System.out.println("Failed to sent put " + e.getRow(i) + ".");
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename))
                .listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * 异步往指定表添加数据
     * @param tablename  	表名
     * @param put	 			需要添加的数据
     * @return long				返回执行时间
     * @throws IOException
     */
    public static long put(String tablename, Put put) throws Exception {
        return put(tablename, Arrays.asList(put));
    }

    /**
     * 往指定表添加数据
     * @param tablename  	表名
     * @param puts	 			需要添加的数据
     * @return long				返回执行时间
     * @throws IOException
     */
    public static long putByHTable(String tablename, List<?> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(tablename));
        htable.setAutoFlushTo(false);
        htable.setWriteBufferSize(5 * 1024 * 1024);
        try {
            htable.put((List<Put>)puts);
            htable.flushCommits();
        } finally {
            htable.close();
            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * 关闭连接
     * @throws IOException
     */
    public static void closeConnect(Connection conn){
        if(null != conn){
            try {
//				conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }
    }

    /**
     * 格式化输出结果
     */
    public static void formatRow(KeyValue[] rs){
        for(KeyValue kv : rs){
            System.out.println(" column family  :  " + Bytes.toString(kv.getFamily()));
            System.out.println(" column   :  " + Bytes.toString(kv.getQualifier()));
            System.out.println(" value   :  " + Bytes.toString(kv.getValue()));
            System.out.println(" timestamp   :  " + String.valueOf(kv.getTimestamp()));
            System.out.println("--------------------");
        }
    }

    /**
     * byte[] 类型的长整形数字转换成 long 类型
     * @param byteNum
     * @return
     */
    public static long bytes2Long(byte[] byteNum) {
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }
}

package com.weimob.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by dell on 2018/11/3.
 */
public class MapR extends Configured implements Tool{
    // step 1: Mapper Class
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapOutputKey = new Text();
        private IntWritable mapOutputValue = new IntWritable(1);

        // map就是从文件中读取数据的<keyvalue>，默认按照文件中一行行读取的
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 第一步：获取每一行的值
            String lineValue = value.toString();

            // 第二步：分割单词，变成<hadoop,1> <spark,1>
            String[] strs = lineValue.split(" ");

            // 第三步：将数组中的每一个单词拿出来组成<key value>，生成加1
            for (String str : strs) {
                // set map out key
                mapOutputKey.set(str);

                // output
                context.write(mapOutputKey, mapOutputValue);

                System.out.println("<" + mapOutputKey + "," + mapOutputValue + ">");
            }
        }

    }

    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("key = " + key);
            // sum
            int sum = 0;

            for (IntWritable value : values) {
                // total
                sum += value.get();

                System.out.print(value.get() + " ");
            }

            System.out.println();
            // set output value
            outputValue.set(sum);

            // output
            context.write(key, outputValue);
        }

    }

    // step 2: Reducer Class
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // sum
            int sum = 0;

            for (IntWritable value : values) {
                // total
                sum += value.get();
            }

            // set output value
            outputValue.set(sum);

            // output
            context.write(key, outputValue);
        }

    }

    /**
     * Execute the command with the given arguments.
     *
     * @param args
     *            command specific arguments.
     * @return exit code.
     * @throws Exception
     *             int run(String [] args) throws Exception;
     */

    // step 3: Driver
    public int run(String[] args) throws Exception {

        // 封装了hadoop的配置项
        Configuration configuration = this.getConf();

        // 设置job
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        // mapreduce程序jar的入口
        job.setJarByClass(this.getClass());

        // input path
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);

        // output path
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        // Mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // ==============shuffle=================
        // 1、分区
        // job.setPartitionerClass(cls);

        // 2、排序
        // job.setSortComparatorClass(cls);

        // 3、分区
        // job.setGroupingComparatorClass(cls);

        // 4、combiner
        job.setCombinerClass(WordCountCombiner.class);
        // ==============shuffle=================

        // Reducer
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置reduce数目
        //job.setNumReduceTasks(2);

        // submit job
        boolean isSuccess = job.waitForCompletion(true);

        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        args = new String[] { "hdfs://sh-ops-hadoop-01-online-09:8020/test01/input",
                "hdfs://sh-ops-hadoop-01-online-09:8020/test01/output01/" };

        Configuration configuration = new Configuration();

        int status = ToolRunner.run(configuration, new MapR(), args);

        System.exit(status);
    }

}

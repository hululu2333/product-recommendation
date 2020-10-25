package com.hu.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 计算商品的共现关系
 * 也就是说根据上一步的结果，计算出哪些商品是共同出现过的
 */
public class GoodsCooccurrenceList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceList(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        //core.xml和hdfs.xml的抽象
        Configuration conf = this.getConf();

        //设置输入输出路径
        Path in = new Path("/output/step1_result");
        Path out = new Path("/output/step2_result");

        //作业的抽象
        Job job = Job.getInstance(conf, "商品之间的共现关系");
        job.setJarByClass(GoodsCooccurrenceList.class);

        //配置mapper
        job.setMapperClass(GoodsCooccurrenceListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //配置reducer
        job.setReducerClass(GoodsCooccurrenceListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * map端输出用户编号和该用户的商品编号列表
     * 和上一个mapreduce的输出结果一样
     */
    static class GoodsCooccurrenceListMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            k2.set(value.toString().split("\t")[0]);
            v2.set(value.toString().split("\t")[1]);

            context.write(k2,v2);
        }
    }

    /**
     * 输出的key是商品编号，输出的value是与该商品共同出现过的商品编号
     */
    static class GoodsCooccurrenceListReducer extends Reducer<Text, Text, Text, Text> {
        Text k3 = new Text();
        Text v3 = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t : values){
                String[] strs = t.toString().split("[,]"); //拿到该用户购买的所有商品的编号

                for(int i=0;i<strs.length-1;i++){
                    for(int j = i+1;j<strs.length;j++){


                        k3.set(strs[i]);
                        v3.set(strs[j]);
                        context.write(k3,v3);
                        context.write(v3,k3); //反着再输出一遍，商品A和商品B共现的同时，商品B也和商品A共现了
                    }
                }
            }
        }
    }
}

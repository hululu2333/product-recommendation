package com.hu.step1;

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
 * 根据用户和商品的喜好关系表得到用户和他所购买的商品列表
 */
public class UserBuyGoodsList extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new UserBuyGoodsList(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        //core.xml和hdfs.xml的抽象
        Configuration conf = this.getConf();

        //设置输入输出路径
        Path in = new Path("/input/data.txt");
        Path out = new Path("/output/step1_result");

        //作业的抽象
        Job job = Job.getInstance(conf, "用户购买的商品列表");
        job.setJarByClass(UserBuyGoodsList.class);

        //配置mapper
        job.setMapperClass(UserBuyGoodsListMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //配置reducer
        job.setReducerClass(UserBuyGoodsListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 输出的key是用户编号，输出的value是商品编号
     */
    static class UserBuyGoodsListMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k2 = new Text();
        private Text v2 = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            k2.set(value.toString().split("[\t]")[0]);
            v2.set(value.toString().split("[\t]")[1]);

            context.write(k2,v2);
        }
    }

    /**
     * 输出的key是用户编号，输出的value是该用户购买过的所有商品的列表
     */
    static class UserBuyGoodsListReducer extends Reducer<Text, Text, Text, Text> {
        private Text k3 = new Text();
        private Text v3 = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            k3 = key;

            StringBuffer sb = new StringBuffer();
            for(Text t : values){
                sb.append(t.toString()).append(",");
            }

            sb.deleteCharAt(sb.length()-1);
            v3.set(sb.toString());

            context.write(k3,v3);
        }
    }
}

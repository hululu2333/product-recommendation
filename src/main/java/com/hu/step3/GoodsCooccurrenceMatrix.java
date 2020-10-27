package com.hu.step3;

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
import java.util.HashMap;

/**
 * 计算商品的共现矩阵
 * 以上一步的结果为输入，得出的结果为 商品编号    共现商品编号：共现次数，共现商品编号，共现次数
 */
public class GoodsCooccurrenceMatrix extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new GoodsCooccurrenceMatrix(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        //core.xml和hdfs.xml的抽象
        Configuration conf = this.getConf();

        //设置输入输出路径
        Path in = new Path("/output/step2_result");
        Path out = new Path("/output/step3_result");

        //作业的抽象
        Job job = Job.getInstance(conf, "商品的共现矩阵");
        job.setJarByClass(GoodsCooccurrenceMatrix.class);

        //配置mapper
        job.setMapperClass(GoodsCooccurrenceMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, in);

        //配置reducer
        job.setReducerClass(GoodsCooccurrenceMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 输出的key是商品编号，输出的value是共现的商品编号
     */
    static class GoodsCooccurrenceMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k2 = new Text();
        private Text v2 = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            k2.set(value.toString().split("\t")[0]);
            v2.set(value.toString().split("\t")[1]);

            context.write(k2,v2);
        }
    }

    /**
     * 输出的key商品编号，输出的value是共现的商品编号和共现次数
     * values中的数据无误
     * map中的数据错了，所有的key都是一样的
     */
    static class GoodsCooccurrenceMatrixReducer extends Reducer<Text, Text, Text, Text> {
        Text k3 = new Text();
        Text v3 = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            k3 = key;

            StringBuffer sb = new StringBuffer();

            HashMap<Text,Integer> map = new HashMap<>(); //用来计算共现次数

            for(Text t : values){
                if(map.containsKey(t)){
                    sb.append("map包含").append(t.toString()).append("将值改为了").
                            append(t.toString()+":"+(map.get(t)+1)).append(",");
                    map.replace(t,map.get(t)+1);
                    sb.append("\n").append("现在的情况是"+map.toString());
                }else{
                    sb.append("map中新添了").append(t.toString()).append(",");
                    map.put(t,1);
                    sb.append("\n").append("现在的情况是"+map.toString());
                }
            }

            sb.append("\n").append("最终的结果为:");
            sb.append(map.toString());


//            StringBuffer sb = new StringBuffer();
//            map.forEach((id,times)->{
//                sb.append(id.toString()).append(":").append(times).append(",");
//            });
//            sb.deleteCharAt(sb.length()-1);
//            v3.set(sb.toString());

            v3.set(sb.toString());

            context.write(k3,v3);
        }
    }
}

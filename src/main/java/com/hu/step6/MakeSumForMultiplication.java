package com.hu.step6;

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
 * @Author ljj
 * @create 2020/8/26 19:31
 */


    public class MakeSumForMultiplication extends Configured implements Tool {


        //private final static Text k = new Text();  // Text类型k可调用set方法

        // private final static IntWritable v = new IntWritable(1); //IntWritable可调用get方法获取到值

        //private final static Text v = new Text();

        @Override
        public int run(String[] args) throws Exception {
            //配置信息
            Configuration conf = getConf();
            String input = "E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\part-r-00000";//数据来源
            //数据最终落户的文件夹是不存在
            String output = "E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result7\\out";//数据去向

            //构建MapReduce，job作业
            Job job = Job.getInstance(conf);
            job.setJobName("MakeSumForMultiplication");
            job.setJarByClass(MakeSumForMultiplication.class);//设置主类

            //设置Mapper函数
            job.setMapperClass(MakeSumForMultiplicationMapper.class);
            //指定输出的Mapper函数的key-value形式
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //设置Ruducer函数
            job.setReducerClass(MakeSumForMultiplicationReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //数据输入形式            key:每行偏移量 value:每行内容
            job.setInputFormatClass(TextInputFormat.class);
            //输出形式
            job.setOutputFormatClass(TextOutputFormat.class);

            //绑定输入输出路径,输入可能会有多个路径
            TextInputFormat.addInputPath(job, new Path(input));


            TextOutputFormat.setOutputPath(job, new Path(output));

            return job.waitForCompletion(true) ? 0 : -1;
        }

        public static class MakeSumForMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {



            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                //直接将一行内容的第一列作为key，第二列作为value，从map输出到reduce
                //将Text转换成字符串，方便处理
                //调用reduce方法时会调用map方法，每读取完一行文字就调用一次map方法，读取完一行文字后，这一行文字就会“消失”，
                //下一次调用map方法又是从第一行读取文字


                String[] tokens = value.toString().split("\t");
                String k=tokens[0];
                String v=tokens[1];





                context.write(new Text(k),new Text(v));




            }


        }


        public static class MakeSumForMultiplicationReducer extends Reducer<Text, Text, Text, Text> {

            @Override           //key:map输出的key   values：key对应的value集合  context；输出最终结果
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int sum=0;
                for (Text v : values) {
                    sum+=Integer.parseInt(v.toString());
                }

                //String value=str.substring(0,str.length()-1);

                context.write(key, new Text(Integer.toString(sum)));
            }


            public static void main(String[] args) throws Exception {
                //              主类对象
                ToolRunner.run(new MakeSumForMultiplication(), args);
            }

        }

    }
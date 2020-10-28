package stepg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author ljj
 * @create 2020/8/27 10:00
 */



public class DuplicateDataForResult extends Configured implements Tool {



    @Override
    public int run(String[] args) throws Exception {
        //配置信息
        Configuration conf = getConf();
        Path input1=new Path("E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\data");
        Path input2=new Path("E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result7\\out\\part-r-00000");
        Path output=new Path("E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result8\\out");
        Job job = Job.getInstance(conf,getClass().getSimpleName());
        job.setJarByClass(DuplicateDataForResult.class);

        job.setMapperClass(FirstMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,input1,TextInputFormat.class,
                FirstMapper.class);
        MultipleInputs.addInputPath(job,input2,TextInputFormat.class,
                SecondMapper.class);

        job.setReducerClass(DuplicateDataForResultReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,output);
        return job.waitForCompletion(true)?0:1;
    }

    public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {



        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //直接将一行内容的第一列作为key，第二列作为value，从map输出到reduce
            //将Text转换成字符串，方便处理
            //调用reduce方法时会调用map方法，每读取完一行文字就调用一次map方法，读取完一行文字后，这一行文字就会“消失”，
            //下一次调用map方法又是从第一行读取文字


            String[] strs = value.toString().split("\t");
            context.write(new Text(strs[0]+"\t"+strs[1]),new Text("\t"));


        }


    }



    public static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {



        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //直接将一行内容的第一列作为key，第二列作为value，从map输出到reduce
            //将Text转换成字符串，方便处理
            //调用reduce方法时会调用map方法，每读取完一行文字就调用一次map方法，读取完一行文字后，这一行文字就会“消失”，
            //下一次调用map方法又是从第一行读取文字


            String[] strs = value.toString().split(" ");
            String[] strs1 = strs[0].split(",");
            context.write(new Text(strs1[0]+"\t"+strs1[1]), new Text(strs1[1]));


        }


    }

    public static class DuplicateDataForResultReducer extends Reducer<Text, Text, Text, Text> {

        @Override           //key:map输出的key   values：key对应的value集合  context；输出最终结果
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Iterator<Text> iterator = values.iterator();
            Text next = iterator.next();
           if(!iterator.hasNext()){
                context.write(key,next);
            }


        }


        public static void main(String[] args) throws Exception {
            //              主类对象
            ToolRunner.run(new DuplicateDataForResult(), args);
        }

    }

}
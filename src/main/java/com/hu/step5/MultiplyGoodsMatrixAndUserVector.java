package stepe;

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

public class MultiplyGoodsMatrixAndUserVector extends Configured implements Tool {

    public static class MultiplyGoodsMatrixAndUserVectorFirstMapper extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            context.write(new Text(lines[0]), new Text("goods-"+lines[1]));
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorSecondMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\t");
            context.write(new Text(lines[0]), new Text("users-"+lines[1]));
        }
    }

    public static class MultiplyGoodsMatrixAndUserVectorReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] g = null;
            String[] u = null;
            for (Text t:values){
                String str = t.toString();
                if (str.startsWith("users")){
                    String[] ls = str.split("-");
                    u = ls[1].split(",");
                }
                if (str.startsWith("goods")){
                    String[] ls = str.split("-");
                    g = ls[1].split(",");
                }
                if (g!=null && u!=null) {
                    for (int i = 0; i < u.length; i++) {
                        String[] user = u[i].split(":");
                        for (int j = 0; j < g.length; j++) {
                            String[] goods = g[j].split(":");
                            int m = Integer.parseInt(user[1]) * Integer.parseInt(goods[1]);
                            String v = user[0]+","+goods[0];
                            context.write(new Text(v), new Text(m+""));
                        }
                    }
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        //配置信息
        Configuration conf  = getConf();
        String FirstInput = "E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result4\\out\\part-r-00000";//数据来源
        Path First = new Path(FirstInput);
        String SecondInput = "E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result5\\out\\part-r-00000";//数据来源
        Path Second = new Path(SecondInput);
        //数据最终落户的文件夹是不存在的
        String output = "E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result6\\out";//数据去向

        //构建MapReduce,job作业
        Job job = Job.getInstance(conf);
        job.setJobName("MultiplyGoodsMatrixAndUserVector");
        job.setJarByClass(MultiplyGoodsMatrixAndUserVector.class);

        //设置Mapper函数
//        job.setMapperClass(MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        //指定输出的Mapper函数的Key-Value形式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置Reducer函数
        job.setReducerClass(MultiplyGoodsMatrixAndUserVectorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //数据输入形式    key:每行偏移量   value:每行内容
        job.setOutputFormatClass(TextOutputFormat.class);

        //绑定输入输出路径,输入可能会产生多个路径
        TextOutputFormat.setOutputPath(job, new Path(output));

        MultipleInputs.addInputPath(job, First, TextInputFormat.class, MultiplyGoodsMatrixAndUserVectorFirstMapper.class);
        MultipleInputs.addInputPath(job, Second, TextInputFormat.class,MultiplyGoodsMatrixAndUserVectorSecondMapper.class);

        return job.waitForCompletion(true)?0:-1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MultiplyGoodsMatrixAndUserVector(), args);
    }

}

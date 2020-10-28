package com.hu.step8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SaveRecommendResultToDB extends Configured implements Tool {
    private static Text k=new Text();

    public static class TableWritable implements WritableComparable, DBWritable{

        private String uid;
        private String gid;
        private int exp;

        public TableWritable() {
        }

        public TableWritable(String uid, String gid, int exp) {
            this.uid = uid;
            this.gid = gid;
            this.exp = exp;
        }

        public void write(PreparedStatement ps) throws SQLException {
            ps.setString(1,uid);
            ps.setString(2,gid);
            ps.setInt(3,exp);
        }

        public void readFields(ResultSet rs) throws SQLException {
            uid = rs.getString(1);
            gid = rs.getString(2);
            exp = rs.getInt(3);
        }

        public int compareTo(Object o) {
            return 0;
        }

        public void write(DataOutput out) throws IOException {

        }

        public void readFields(DataInput in) throws IOException {

        }
    }

    public static class SaveRecommendResultToDBMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines[] = value.toString().split("\t");
            //k.set(lines[0]+","+lines[1]);
            context.write(new Text(lines[0]),new Text(lines[1]));
        }
    }


    public static class SaveRecommendResultToDBReducer extends Reducer<Text, Text, TableWritable, TableWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t:values){
                String[] keys = key.toString().split(",");

                context.write(new TableWritable(keys[0],keys[1],Integer.parseInt(t.toString())),null);
            }
        }
    }

    public int run(String[] args) throws Exception {
        //配置信息
        Configuration conf  = new Configuration();
        String input = "E:\\software2\\ideaProject\\hadoop1\\src\\test\\java\\step1\\result8\\out\\part-r-00000";//数据来源

        //获取数据库连接
        DBConfiguration.configureDB(conf,"com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://127.0.0.1:3306/grms?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false",
                "root","password");

        //构建MapReduce,job作业
        Job job = Job.getInstance(conf);
        job.setJobName("SaveRecommendResultToDB");
        job.setJarByClass(SaveRecommendResultToDB.class);

        //设置Mapper函数
        job.setMapperClass(SaveRecommendResultToDBMapper.class);
        //指定输出的Mapper函数的Key-Value形式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置Reducer函数
        job.setReducerClass(SaveRecommendResultToDBReducer.class);
        job.setOutputKeyClass(TableWritable.class);
        job.setOutputValueClass(TableWritable.class);

        //数据输入形式    key:每行偏移量   value:每行内容
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        //绑定输入输出路径,输入可能会产生多个路径
        TextInputFormat.addInputPath(job, new Path(input));
        DBOutputFormat.setOutput(job,"results","uid","gid","exp");

        return job.waitForCompletion(true)?0:-1;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SaveRecommendResultToDB(),args);
    }
}
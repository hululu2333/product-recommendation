package stepe;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//组合键，在原先key的基础上+flag
public class TextTuple
        implements WritableComparable<TextTuple> {
    private Text key;//20001
    private LongWritable flag;//标志：给不同文件内容固定顺序

    public TextTuple() {
    }

    public TextTuple(Text key, LongWritable flag) {
        this.key = key;
        this.flag = flag;
    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public LongWritable getFlag() {
        return flag;
    }

    public void setFlag(LongWritable flag) {
        this.flag = flag;
    }

    @Override//排序
    public int compareTo(TextTuple o) {
        int num=this.key.compareTo(o.key);
        if(num==0){
            return this.flag.compareTo(o.flag);
        }
        return num;
    }

    @Override//序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.key.toString());
        out.writeLong(this.flag.get());
    }

    @Override//反序列化
    public void readFields(DataInput in) throws IOException {
        this.flag=new LongWritable(in.readLong());
        this.key=new Text(in.readUTF());
    }
}

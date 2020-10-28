package stepe;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

//分区
public class KeyPartitionor extends Partitioner<TextTuple, Text> {
    @Override
    public int getPartition(
            TextTuple textTuple, //map输出的key
            Text text, //map输出的value
            int numPartitions) {//Reduce个数

        return textTuple.getKey().
                hashCode()*Integer.MAX_VALUE
                %numPartitions;
    }
}

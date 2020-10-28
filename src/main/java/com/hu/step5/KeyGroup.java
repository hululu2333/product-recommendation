package com.hu.step5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroup extends WritableComparator {
    public KeyGroup() {
        //在父类注册分区
        super(TextTuple.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextTuple t1=(TextTuple)a;
        TextTuple t2=(TextTuple)b;
        //key:20001-flag:0/1
        return t1.getKey().compareTo(t2.getKey());

    }
}

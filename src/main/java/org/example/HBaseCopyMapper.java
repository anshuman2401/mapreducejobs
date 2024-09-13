package org.example;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import java.io.IOException;

public class HBaseCopyMapper extends TableMapper<ImmutableBytesWritable, Result> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Setting up HBaseCopyMapper");
        super.setup(context);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}



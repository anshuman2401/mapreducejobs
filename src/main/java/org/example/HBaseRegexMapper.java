package org.example;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.regex.Pattern;

public class HBaseRegexMapper extends TableMapper<Text, Text> {
    private Pattern pattern;



    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String regex = context.getConfiguration().get("hbase.regex");
        pattern = Pattern.compile(regex);
    }

    @Override
    public void map(ImmutableBytesWritable rowKey, Result value, Context context) throws IOException, InterruptedException {
        String key = new String(rowKey.get());

        if (pattern.matcher(key).matches()) {
            context.write(new Text(key), new Text(""));
        }
    }
}

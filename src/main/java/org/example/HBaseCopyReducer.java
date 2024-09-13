package org.example;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HBaseCopyReducer extends Reducer<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result> {
    private Connection connection = null;
    private Table table = null;
    private byte[] columnFamily;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Setting up HBaseCopyMapper");
        super.setup(context);
        connection = ConnectionFactory.createConnection(context.getConfiguration());
        table = connection.getTable(TableName.valueOf(context.getConfiguration().get("hbase.table.destination")));
        columnFamily = Bytes.toBytes(context.getConfiguration().get("hbase.column.family"));
    }

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException {
        for (Result value : values) {
            Put put = new Put(key.get());
            for (org.apache.hadoop.hbase.Cell cell : value.rawCells()) {
                if (Bytes.equals(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), columnFamily, 0, columnFamily.length)) {
                    put.add(cell);
                }
            }
            table.put(put);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.cleanup(context);
    }
}



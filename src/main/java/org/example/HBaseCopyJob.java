package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBaseCopyJob {
    public static void main(String[] args) throws Exception {
        System.out.println("Source Table: " + args[0]);
        System.out.println("Destination Table: " + args[1]);
        System.out.println("Output path: " + args[2]);
        System.out.println("Column Family: " + args[3]);

        if (args.length != 4) {
            System.err.println("Usage: HBaseCopyJob <source-table> <destination-table> <output-path> <column-family>");
            System.exit(-1);
        }

        Configuration conf = HBaseUtils.getHbaseConfiguration();
        conf.set("hbase.table.destination", args[1]);
        conf.set("hbase.column.family", args[3]);

        Job job = Job.getInstance(conf, "HBase Copy Job");
        job.setJarByClass(HBaseCopyJob.class);


        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(args[3]));
        TableMapReduceUtil.initTableMapperJob(args[0], scan, HBaseCopyMapper.class, Text.class, Text.class, job);
        job.setReducerClass(HBaseCopyReducer.class);
        job.setOutputFormatClass(FileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


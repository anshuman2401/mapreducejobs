package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HBaseRegexJob {
    public static void main(String[] args) throws Exception {

        for (String arg: args) {
            System.out.println("Args: " + arg);
        }

        if (args.length != 3) {
            System.err.println("Usage: HBaseRegexJob <table> <regex> <output>");
            System.exit(-1);
        }

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "test0,test1,test2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.regex", args[1]);
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "test:8032");


        Job job = Job.getInstance(conf, "HBase Regex Job");
        job.setJarByClass(HBaseRegexJob.class);

        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(args[0], scan, HBaseRegexMapper.class, Text.class, Text.class, job);
        job.setReducerClass(HBaseRegexReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

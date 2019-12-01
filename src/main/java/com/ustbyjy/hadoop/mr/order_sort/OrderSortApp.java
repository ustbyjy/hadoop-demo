package com.ustbyjy.hadoop.mr.order_sort;

import org.apache.avro.JsonProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderSortApp {

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            args = new String[]{"data/GroupingComparator.txt", "/tmp/order_sort"};
        }

        Configuration configuration = new Configuration();

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("delete output path success.");
        }

        Job job = Job.getInstance(configuration, "OrderSort");

        job.setJarByClass(OrderSortApp.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setMapperClass(OrderSortMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderSortGroupingComparator.class);

        job.setReducerClass(OrderSortReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(JsonProperties.Null.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }


}

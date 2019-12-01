package com.ustbyjy.hadoop.mr.word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountApp {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 接收到的每一行数据
            String line = value.toString();

            // 按照指定分隔符进行拆分
            String[] words = line.split(" ");

            // 通过上下文把map的处理结果输出
            for (String word : words) {
                context.write(new Text(word), one);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // 求key出现的次数总和
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            args = new String[]{"data/word.txt", "/tmp/word_count"};
        }

        Configuration configuration = new Configuration();

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
            System.out.println("delete output path success.");
        }

        Job job = Job.getInstance(configuration, "WordCount");

        job.setJarByClass(WordCountApp.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 对每一个MapTask的输出进行局部汇总，以减小网络传输量即采用Combiner功能
        job.setCombinerClass(MyReducer.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}

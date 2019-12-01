package com.ustbyjy.hadoop.mr.order_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 接收到的每一行数据
        String line = value.toString();

        // 按照指定分隔符进行拆分
        String[] fields = line.split("\t");

        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

        context.write(k, NullWritable.get());
    }
}

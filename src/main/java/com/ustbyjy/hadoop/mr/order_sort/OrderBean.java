package com.ustbyjy.hadoop.mr.order_sort;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId; // 订单id号
    private double price; // 价格

    public OrderBean() {
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        int result;
        if (this.orderId > o.getOrderId()) {
            result = 1;
        } else if (this.orderId < o.getOrderId()) {
            result = -1;
        } else {
            result = this.price > o.getPrice() ? -1 : 1;
        }

        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readInt();
        this.price = dataInput.readDouble();
    }
}

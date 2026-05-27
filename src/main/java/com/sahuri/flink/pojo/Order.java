package com.sahuri.flink.pojo;


import java.math.BigDecimal;

public class Order {
    public int orderId;
    public int customerId;
    public BigDecimal amount;

    public Order() {}

    public Order(int orderId, int customerId, BigDecimal amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
    }
}

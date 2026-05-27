package com.sahuri.flink.pojo;

import java.math.BigDecimal;

public class CustomerOrder {
    public int orderId;
    public int customerId;
    public String customerName;
    public String city;
    public BigDecimal amount;

    public CustomerOrder() {}

    public CustomerOrder(int orderId, int customerId, String customerName, String city, BigDecimal amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.customerName = customerName;
        this.city = city;
        this.amount = amount;
    }
}

package com.sahuri.flink.process;

import com.sahuri.flink.pojo.Customer;
import com.sahuri.flink.pojo.CustomerOrder;
import com.sahuri.flink.pojo.Order;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class JoinCustomerOrderFunction extends CoProcessFunction<Customer, Order, CustomerOrder> {

    private transient ValueState<Customer> customerState;
    private transient ValueState<Order> orderState;

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Time.hours(6))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Customer> customerDesc = new ValueStateDescriptor<>("customer", Customer.class);
        customerDesc.enableTimeToLive(ttl);
        customerState = getRuntimeContext().getState(customerDesc);

        ValueStateDescriptor<Order> orderDesc = new ValueStateDescriptor<>("order", Order.class);
        orderDesc.enableTimeToLive(ttl);
        orderState = getRuntimeContext().getState(orderDesc);
    }

    @Override
    public void processElement1(Customer customer, Context ctx, Collector<CustomerOrder> out) {
        try {
            customerState.update(customer);
            Order order = orderState.value();
            if (order != null) {
                out.collect(new CustomerOrder(order.orderId, customer.id, customer.name, customer.city, order.amount));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed processing customer id=" + customer.id, e);
        }
    }

    @Override
    public void processElement2(Order order, Context ctx, Collector<CustomerOrder> out) {
        try {
            orderState.update(order);
            Customer customer = customerState.value();
            if (customer != null) {
                out.collect(new CustomerOrder(order.orderId, customer.id, customer.name, customer.city, order.amount));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed processing order id=" + order.orderId, e);
        }
    }
}


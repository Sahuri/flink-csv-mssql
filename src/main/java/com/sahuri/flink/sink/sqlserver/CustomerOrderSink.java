package com.sahuri.flink.sink.sqlserver;

import com.sahuri.flink.GlobalConfig;
import com.sahuri.flink.pojo.CustomerOrder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.PreparedStatement;

public class CustomerOrderSink {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerOrderSink.class);
    public static SinkFunction<CustomerOrder> create() {
        String url = required("jdbc.url");
        String driver = required("jdbc.driver");
        String username = required("jdbc.username");
        String password = required("jdbc.password");

        int batchSize = Integer.parseInt(required("batch.size"));
        int batchIntervalMs = Integer.parseInt(required("batch.interval.ms"));

        LOG.info("Creating SQLServer sink url={} batchSize={} batchIntervalMs={}", url, batchSize, batchIntervalMs);

        return JdbcSink.sink(
                "INSERT INTO dbo.CustomerOrder (order_id, customer_id,customer_name,city,amount) VALUES (?, ?, ?,?,?)",
                (PreparedStatement ps, CustomerOrder customerOrder) -> {
                    ps.setInt(1, customerOrder.orderId);
                    ps.setInt(2, customerOrder.customerId);
                    ps.setString(3, customerOrder.customerName);
                    ps.setString(4, customerOrder.city);
                    ps.setBigDecimal(5, customerOrder.amount);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName(driver)
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }

    private static String required(String key) {
        String value = GlobalConfig.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required config: " + key);
        }
        return value;
    }
}

package com.sahuri.flink;

import com.sahuri.flink.pojo.Customer;
import com.sahuri.flink.pojo.CustomerOrder;
import com.sahuri.flink.pojo.Order;
import com.sahuri.flink.process.JoinCustomerOrderFunction;
import com.sahuri.flink.sink.sqlserver.CustomerOrderSink;
import com.sahuri.flink.source.CsvFolder;
import com.sahuri.flink.source.CustomerCsvParser;
import com.sahuri.flink.source.OrderCsvParser;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Application {

    public static void main(String[] args) throws Exception {

        // === 1️⃣ Setup environment ===
        String envName = System.getProperty("flink.env", "uat");
        GlobalConfig.load(envName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Integer.parseInt(GlobalConfig.get("env.parallelism")));
        env.enableCheckpointing(Integer.parseInt(GlobalConfig.get("checkpoint.interval.ms")), CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(GlobalConfig.get("checkpoint.path"));
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // === 2️⃣ Create sources ===
        DataStream<Customer> customers = CsvFolder.readCsvAsStreamSafe(
                env,
                GlobalConfig.get("csv.path.customers"),
                "id",
                "customer",
                new CustomerCsvParser()
        );
        DataStream<Order> orders = CsvFolder.readCsvAsStreamSafe(
                env,
                GlobalConfig.get("csv.path.orders"),
                "order_id",
                "order",
                new OrderCsvParser()
        );

        // === 3️⃣ Join by customerId ===
        DataStream<CustomerOrder> joined = customers
                .connect(orders)
                .keyBy(c -> c.id, o -> o.customerId)
                .process(new JoinCustomerOrderFunction());

        joined.addSink(CustomerOrderSink.create());

        // === 5️⃣ Execute ===
        env.execute("Join Customers and Orders CSV");
    }
}

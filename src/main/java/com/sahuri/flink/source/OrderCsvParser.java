package com.sahuri.flink.source;

import com.sahuri.flink.pojo.Order;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

public class OrderCsvParser extends CsvParseMetrics.ParsingFlatMap<Order> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderCsvParser.class);
    private static final int EXPECTED_COLUMNS = 3;

    public OrderCsvParser() {
        super("order");
    }

    @Override
    protected boolean tryParse(String line, Collector<Order> out) {
        try {
            List<String> fields = CsvLine.split(CsvHeader.normalize(line));
            if (fields.size() < EXPECTED_COLUMNS) {
                LOG.warn("Skip bad order row (expected {} cols): {}", EXPECTED_COLUMNS, line);
                return false;
            }
            int orderId = Integer.parseInt(fields.get(0).trim());
            int customerId = Integer.parseInt(fields.get(1).trim());
            BigDecimal amount = new BigDecimal(fields.get(2).trim());
            out.collect(new Order(orderId, customerId, amount));
            return true;
        } catch (Exception e) {
            LOG.warn("Skip bad order row: {} error={}", line, e.toString());
            return false;
        }
    }
}

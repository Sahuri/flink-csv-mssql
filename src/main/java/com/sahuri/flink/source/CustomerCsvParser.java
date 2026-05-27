package com.sahuri.flink.source;

import com.sahuri.flink.pojo.Customer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CustomerCsvParser extends CsvParseMetrics.ParsingFlatMap<Customer> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerCsvParser.class);
    private static final int EXPECTED_COLUMNS = 3;

    public CustomerCsvParser() {
        super("customer");
    }

    @Override
    protected boolean tryParse(String line, Collector<Customer> out) {
        try {
            List<String> fields = CsvLine.split(CsvHeader.normalize(line));
            if (fields.size() < EXPECTED_COLUMNS) {
                LOG.warn("Skip bad customer row (expected {} cols): {}", EXPECTED_COLUMNS, line);
                return false;
            }
            int id = Integer.parseInt(fields.get(0).trim());
            String name = fields.get(1);
            String city = fields.get(2);
            out.collect(new Customer(id, name, city));
            return true;
        } catch (Exception e) {
            LOG.warn("Skip bad customer row: {} error={}", line, e.toString());
            return false;
        }
    }
}

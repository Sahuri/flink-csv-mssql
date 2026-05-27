package com.sahuri.flink.source;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

/**
 * Flink metric counters for CSV parsing.
 */
public final class CsvParseMetrics {
    public static final String GROUP_RECORD_TYPE = "recordType";
    public static final String COUNTER_HEADER_SKIPPED = "rows_skipped_header";
    public static final String COUNTER_PARSE_SKIPPED = "rows_skipped_parse";
    public static final String COUNTER_PARSED_OK = "rows_parsed_ok";

    private CsvParseMetrics() {}

    public static RichFilterFunction<String> headerSkipFilter(String recordType, String headerToken) {
        return new RichFilterFunction<String>() {
            private transient Counter headerSkipped;

            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) {
                headerSkipped = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup(GROUP_RECORD_TYPE, recordType)
                        .counter(COUNTER_HEADER_SKIPPED);
            }

            @Override
            public boolean filter(String line) {
                if (CsvHeader.isHeaderRow(line, headerToken)) {
                    headerSkipped.inc();
                    return false;
                }
                return true;
            }
        };
    }

    public abstract static class ParsingFlatMap<T> extends RichFlatMapFunction<String, T> {
        private final String recordType;
        private transient Counter parseSkipped;
        private transient Counter parsedOk;

        protected ParsingFlatMap(String recordType) {
            this.recordType = recordType;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            var group = getRuntimeContext().getMetricGroup().addGroup(GROUP_RECORD_TYPE, recordType);
            parseSkipped = group.counter(COUNTER_PARSE_SKIPPED);
            parsedOk = group.counter(COUNTER_PARSED_OK);
        }

        @Override
        public final void flatMap(String line, Collector<T> out) {
            if (tryParse(line, out)) {
                parsedOk.inc();
            } else {
                parseSkipped.inc();
            }
        }

        /** @return true when a record was emitted */
        protected abstract boolean tryParse(String line, Collector<T> out);
    }
}

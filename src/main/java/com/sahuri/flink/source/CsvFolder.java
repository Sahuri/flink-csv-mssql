package com.sahuri.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvFolder {
    private static final Logger LOG = LoggerFactory.getLogger(CsvFolder.class);
    private static final String INPUT_FOLDER = "/input/";
    private static final String PROCESSED_FOLDER = "/processed/";

    public static <T> DataStream<T> readCsvAsStream(
            StreamExecutionEnvironment env,
            String csvFolderPath,
            String headerToken,
            MapFunction<String, T> parser) {

        DataStream<String> lines = createLineStream(env, csvFolderPath, headerToken);
        return lines.map(parser);
    }

    public static <T> DataStream<T> readCsvAsStreamSafe(
            StreamExecutionEnvironment env,
            String csvFolderPath,
            String headerToken,
            String recordType,
            FlatMapFunction<String, T> parser) {

        DataStream<String> lines = createLineStream(env, csvFolderPath, headerToken, recordType);
        return lines.flatMap(parser);
    }

    private static DataStream<String> createLineStream(
            StreamExecutionEnvironment env,
            String csvFolderPath,
            String headerToken) {
        return createLineStream(env, csvFolderPath, headerToken, null);
    }

    private static DataStream<String> createLineStream(
            StreamExecutionEnvironment env,
            String csvFolderPath,
            String headerToken,
            String recordType) {

        String inputPath = csvFolderPath + INPUT_FOLDER;
        String processedPath = csvFolderPath + PROCESSED_FOLDER;

        FileSource<String> source = FileSource
                .forRecordStreamFormat(new TextLineFormat(), new Path(inputPath))
                .monitorContinuously(java.time.Duration.ofSeconds(10))
                .build();

        DataStream<String> streamLine = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "CSV Source: " + csvFolderPath)
                .filter(line -> line != null && !line.isBlank());

        if (headerToken != null && !headerToken.isBlank()) {
            if (recordType != null) {
                streamLine = streamLine.filter(CsvParseMetrics.headerSkipFilter(recordType, headerToken));
            } else {
                streamLine = streamLine.filter(line -> !CsvHeader.isHeaderRow(line, headerToken));
            }
        }

        LOG.info("CSV folder source created. inputPath={} processedPath={}", inputPath, processedPath);
        streamLine.addSink(new MoveFileSink(inputPath, processedPath));
        return streamLine;
    }
}

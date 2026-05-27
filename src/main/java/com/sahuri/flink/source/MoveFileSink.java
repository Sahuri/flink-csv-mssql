package com.sahuri.flink.source;


import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Sink untuk memindahkan file ke folder processed/
public class MoveFileSink implements SinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MoveFileSink.class);
    private final String inputDir;
    private final String processedDir;
    private static final Set<String> MOVED_FILES = ConcurrentHashMap.newKeySet();

    public MoveFileSink(String inputDir, String processedDir) {
        this.inputDir = stripFileScheme(inputDir);
        this.processedDir = stripFileScheme(processedDir);
    }

    @Override
    public void invoke(String value,Context context) {
        // Catatan: di sini kita tidak tahu file mana tepatnya,
        // tapi kita bisa memindahkan semua file lama di inputDir setelah dibaca
        try {
            File sourceFolder = new File(inputDir);
            File[] csvFiles = sourceFolder.listFiles((dir, name) -> name.endsWith(".csv"));

            if (csvFiles != null) {
                for (File file : csvFiles) {
                    String fileName = file.getName();
                    if (!MOVED_FILES.add(inputDir + "|" + fileName)) {
                        continue; // already moved (or in-flight) during this JVM lifetime
                    }
                    File dest = new File(processedDir + "/" + fileName);
                    Files.createDirectories(dest.getParentFile().toPath());
                    Files.move(file.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    LOG.info("Moved file {} -> {}", file.getAbsolutePath(), dest.getAbsolutePath());
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to move processed CSV files: {}", e.getMessage());
        }
    }

    private static String stripFileScheme(String path) {
        if (path == null) return null;
        return path.replace("file://", "");
    }
}

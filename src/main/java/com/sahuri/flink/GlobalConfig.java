package com.sahuri.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GlobalConfig {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalConfig.class);
    private static final Properties PROPS = new Properties();

    private GlobalConfig() {
        // Prevent instantiation
    }

    public static void load(String env) {
        String fileName = String.format("flink-%s.properties", env);
        try (InputStream input = GlobalConfig.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                throw new RuntimeException("Configuration file not found: " + fileName);
            }
            PROPS.load(input);
            LOG.info("✅ Loaded configuration: {}",fileName);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + fileName, e);
        }
    }

    public static String get(String key) {
        return PROPS.getProperty(key);
    }

    public static String get(String key, String defaultValue) {
        return PROPS.getProperty(key, defaultValue);
    }
}


package com.sahuri.flink.source;

import java.util.ArrayList;
import java.util.List;

/**
 * Small dependency-free CSV splitter.
 * Supports:
 * - comma delimiter
 * - quoted fields with commas
 * - escaped quotes inside quoted field via double-quote ("")
 */
public final class CsvLine {
    private CsvLine() {}

    public static List<String> split(String line) {
        List<String> out = new ArrayList<>();
        if (line == null) return out;

        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;
        while (i < line.length()) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // escaped quote
                    cur.append('"');
                    i += 2;
                    continue;
                }
                inQuotes = !inQuotes;
                i++;
                continue;
            }

            if (c == ',' && !inQuotes) {
                out.add(cur.toString().trim());
                cur.setLength(0);
                i++;
                continue;
            }

            cur.append(c);
            i++;
        }

        out.add(cur.toString().trim());
        return out;
    }
}


package com.sahuri.flink.source;

import java.util.List;
import java.util.Locale;

/**
 * Header-row detection helpers (BOM, trim, case-insensitive first-column match).
 */
public final class CsvHeader {
    private static final char UTF8_BOM = '\uFEFF';

    private CsvHeader() {}

    public static String normalize(String line) {
        if (line == null) {
            return "";
        }
        return stripBom(line).trim();
    }

    public static String stripBom(String line) {
        if (line == null || line.isEmpty()) {
            return line;
        }
        if (line.charAt(0) == UTF8_BOM) {
            return line.substring(1);
        }
        return line;
    }

    /**
     * Returns true when the line looks like a CSV header row for the given token.
     * Matches when the first column equals the token (case-insensitive), or when the
     * normalized line starts with the token followed by a comma.
     */
    public static boolean isHeaderRow(String line, String headerToken) {
        if (headerToken == null || headerToken.isBlank()) {
            return false;
        }

        String normalized = normalize(line);
        if (normalized.isEmpty()) {
            return false;
        }

        String token = headerToken.trim().toLowerCase(Locale.ROOT);
        String lowerLine = normalized.toLowerCase(Locale.ROOT);

        if (lowerLine.equals(token)) {
            return true;
        }
        if (lowerLine.startsWith(token + ",")) {
            return true;
        }

        List<String> fields = CsvLine.split(normalized);
        if (!fields.isEmpty()) {
            return fields.get(0).trim().toLowerCase(Locale.ROOT).equals(token);
        }

        return false;
    }
}

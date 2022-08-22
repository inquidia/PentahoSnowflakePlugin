package org.inquidia.kettle.plugins.snowflakeplugin.bulkloader;

import org.junit.Ignore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SnowflakeBulkLoaderTest {

    @Test
    @Disabled
    void containsSeparatorOrEnclosure() {
    }

    @Test
    void searchForEscapableChar_normalString() {
        String source = "this is a string";
        String searchStr = "is";
        assertTrue(SnowflakeBulkLoader.searchForEscapableChar(source.getBytes(StandardCharsets.UTF_8), searchStr.getBytes(StandardCharsets.UTF_8), 5));
    }

    @Test
    void searchForEscapableChar_newLine() {
        String source = "this is a " + System.lineSeparator() + "string";
        String searchStr = System.lineSeparator();
        assertTrue(SnowflakeBulkLoader.searchForEscapableChar(source.getBytes(StandardCharsets.UTF_8), searchStr.getBytes(StandardCharsets.UTF_8), 10));
    }

    @Test
    void searchForEscapableChar_csvDelimiter() {
        String source = "this is a " + SnowflakeBulkLoaderMeta.CSV_DELIMITER + "string";
        String searchStr = SnowflakeBulkLoaderMeta.CSV_DELIMITER;
        assertTrue(SnowflakeBulkLoader.searchForEscapableChar(source.getBytes(StandardCharsets.UTF_8), searchStr.getBytes(StandardCharsets.UTF_8), 10));
    }

    @Test
    void searchForEscapableChar_csvRecordDelimiter() {
        String source = "this is a " + SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER + "string";
        String searchStr = SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER;
        assertTrue(SnowflakeBulkLoader.searchForEscapableChar(source.getBytes(StandardCharsets.UTF_8), searchStr.getBytes(StandardCharsets.UTF_8), 10));
    }

    @Test
    void searchForEscapableChar_csvEscapeChar() {
        String source = "this is a " + SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR + "string";
        String searchStr = SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR;
        assertTrue(SnowflakeBulkLoader.searchForEscapableChar(source.getBytes(StandardCharsets.UTF_8), searchStr.getBytes(StandardCharsets.UTF_8), 10));
    }

    @Test
    void searchForEscapableChar_csvEnclosure() {
        String source = "this is a " + SnowflakeBulkLoaderMeta.ENCLOSURE + "string";
        String searchStr = SnowflakeBulkLoaderMeta.ENCLOSURE;
        assertTrue(SnowflakeBulkLoader.searchForEscapableChar(source.getBytes(StandardCharsets.UTF_8), searchStr.getBytes(StandardCharsets.UTF_8), 10));
    }
}
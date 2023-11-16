package com.alibaba.datax.plugin.unstructuredstorage.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.stream.Collectors;

public class SqlWriter implements UnstructuredWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SqlWriter.class);

    private Writer sqlWriter;
    private String quoteChar;
    private String lineSeparator;
    private String tableName;
    private StringBuilder insertPrefix;
    private String encoding;

    public SqlWriter(Writer writer, String quoteChar, String tableName, String lineSeparator, List<String> columnNames, String encoding) {
        this.sqlWriter = writer;
        this.quoteChar = quoteChar;
        this.lineSeparator = lineSeparator;
        this.tableName = tableName;
        this.encoding = encoding;
        buildInsertPrefix(columnNames);
    }

    @Override
    public long writeOneRecord(List<String> splitedRows) throws IOException {
        if (splitedRows.isEmpty()) {
            LOG.info("Found one record line which is empty.");
            return 0L;
        }

        StringBuilder sqlPatten = new StringBuilder(4096).append(insertPrefix);
        sqlPatten.append(splitedRows.stream().map(e -> {
            if (e == null) {
                return "NULL";
            }
            return "'" + DataXCsvWriter.replace(e, "'", "''") + "'";
        }).collect(Collectors.joining(",")));
        sqlPatten.append(");").append(lineSeparator);
        String str = sqlPatten.toString();
        this.sqlWriter.write(str);
        return str.getBytes().length;
    }

    private void buildInsertPrefix(List<String> columnNames) {
        StringBuilder sb = new StringBuilder(columnNames.size() * 32);

        for (String columnName : columnNames) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(quoteChar).append(columnName).append(quoteChar);
        }

        int capacity = 16 + tableName.length() + sb.length();
        this.insertPrefix = new StringBuilder(capacity);
        this.insertPrefix.append("INSERT INTO ").append(tableName).append(" (").append(sb).append(")").append(" VALUES(");
    }

    public void appendCommit() throws IOException {
        this.sqlWriter.write("commit;" + lineSeparator);
    }

    @Override
    public void flush() throws IOException {
        this.sqlWriter.flush();
    }

    @Override
    public void close() throws IOException {
        this.sqlWriter.close();
    }
}

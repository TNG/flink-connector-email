package com.tngtech.flink.connector.email.testing;

import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import com.icegreen.greenmail.junit4.GreenMailRule;
import com.icegreen.greenmail.util.ServerSetup;
import com.tngtech.flink.connector.email.imap.ReadableMetadata;
import com.tngtech.flink.connector.email.smtp.WritableMetadata;
import jakarta.mail.Session;
import lombok.RequiredArgsConstructor;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.flink.table.utils.EncodingUtils.escapeIdentifier;
import static org.apache.flink.table.utils.EncodingUtils.escapeSingleQuotes;
import static org.awaitility.Awaitility.await;

public class TestBase {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster = getCluster();

    // Use 4xxx instead of the default 3xxx to avoid interference with a running standalone image
    public static final ServerSetup IMAP = new ServerSetup(4143, null, "imap");
    public static final ServerSetup IMAPS = new ServerSetup(4993, null, "imaps");
    public static final ServerSetup SMTP = new ServerSetup(4025, null, "smtp");

    private static final Duration DEFAULT_DURATION = Duration.ofSeconds(10);

    @Rule
    public final GreenMailRule greenMailRule =
        new GreenMailRule(new ServerSetup[] {IMAP, IMAPS, SMTP})
            .withConfiguration(getGreenmailConfiguration());

    public Session session;
    public StreamExecutionEnvironment executionEnv;
    public StreamTableEnvironment tEnv;

    @Before
    public void before() throws Exception {
        greenMailRule.purgeEmailFromAllMailboxes();
        session = greenMailRule.getImap().createSession();

        executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnv.setParallelism(1);

        tEnv = StreamTableEnvironment.create(executionEnv);
    }

    private static MiniClusterWithClientResource getCluster() {
        return new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(1)
                .setNumberTaskManagers(1)
                .build());
    }

    /**
     * Can be overridden by sub classes.
     */
    public GreenMailConfiguration getGreenmailConfiguration() {
        return GreenMailConfiguration.aConfig().withDisabledAuthentication();
    }

    /**
     * Collects at most {@param expectedRows} rows.
     *
     * <p>If the given number of rows have been collected, it stops collecting, which means unintended
     * further rows will not be collected. If fewer than expected rows are returned, it fails the
     * test.
     */
    public List<Row> collect(TableResult tableResult, int expectedRows) throws Exception {
        return collect(tableResult, expectedRows, DEFAULT_DURATION);
    }

    /**
     * Collects at most {@param expectedRows} rows, with a timeout of {@param maxTime}.
     *
     * <p>If the given number of rows have been collected, it stops collecting, which means unintended
     * further rows will not be collected. If fewer than expected rows are returned, it fails the
     * test.
     */
    public List<Row> collect(TableResult tableResult, int expectedRows, Duration maxTime)
        throws Exception {

        final List<Row> rows = new ArrayList<>();
        try (final CloseableIterator<Row> it = tableResult.collect()) {
            final Callable<Boolean> predicate = () -> rows.size() >= expectedRows;

            final CollectorThread collectorThread = new CollectorThread(it, rows, predicate);
            collectorThread.start();

            try {
                await().atMost(maxTime).until(predicate);
            } finally {
                collectorThread.interrupt();
            }
        }

        if (rows.size() < expectedRows) {
            Assert.fail(String.format("Expected %d rows, but only got %d before the timeout.",
                expectedRows, rows.size()));
        }

        return rows;
    }

    // ---------------------------------------------------------------------------------------------

    public Catalog createImapCatalog(String name) throws Exception {
        return createImapCatalog(name, Collections.emptyMap());
    }

    public Catalog createImapCatalog(String name, Map<String, String> options) throws Exception {
        Map<String, String> allOptions = new HashMap<>();
        allOptions.put("type", "imap");
        allOptions.put("host", "localhost");
        allOptions.put("port", String.valueOf(IMAP.getPort()));
        allOptions.putAll(options);

        tEnv.executeSql(String.format("CREATE CATALOG %s WITH (%s)",
                escapeIdentifier(name),
                allOptions.entrySet().stream()
                    .map(entry -> String.format("'%s' = '%s'",
                        escapeSingleQuotes(entry.getKey()),
                        escapeSingleQuotes(entry.getValue())))
                    .collect(Collectors.joining(","))))
            .await();

        return tEnv.getCatalog(name).orElseThrow(() -> new IllegalStateException(
            String.format("Expected catalog '%s' to exist.", name)));
    }

    public Table createImapSource(String name, ResolvedSchema schema) throws Exception {
        return createImapSource(name, schema, Collections.emptyMap());
    }

    public Table createImapSource(String name, ResolvedSchema schema, Map<String, String> options)
        throws Exception {
        Map<String, String> allOptions = new HashMap<>();
        allOptions.put("connector", "imap");
        allOptions.put("host", "localhost");
        allOptions.put("port", String.valueOf(IMAP.getPort()));
        allOptions.putAll(options);

        createTable(name, schema, allOptions);
        return tEnv.from(name);
    }

    public void createSmtpSink(String name, ResolvedSchema schema) throws Exception {
        createSmtpSink(name, schema, Collections.emptyMap());
    }

    public void createSmtpSink(String name, ResolvedSchema schema, Map<String, String> options)
        throws Exception {
        Map<String, String> allOptions = new HashMap<>();
        allOptions.put("connector", "smtp");
        allOptions.put("host", "localhost");
        allOptions.put("port", String.valueOf(SMTP.getPort()));
        allOptions.putAll(options);

        createTable(name, schema, allOptions);
    }

    public Column metadataColumn(ReadableMetadata metadata) {
        return Column.metadata(metadata.getKey(), metadata.getType(), metadata.getKey(), false);
    }

    public Column metadataColumn(WritableMetadata metadata) {
        return Column.metadata(metadata.getKey(), metadata.getType(), metadata.getKey(), false);
    }

    private void createTable(String name, ResolvedSchema schema, Map<String, String> options)
        throws Exception {
        tEnv.executeSql(String.format("CREATE TEMPORARY TABLE %s %s WITH (%s)",
                escapeIdentifier(name),
                schema,
                options.entrySet().stream()
                    .map(entry -> String.format("'%s' = '%s'",
                        escapeSingleQuotes(entry.getKey()),
                        escapeSingleQuotes(entry.getValue())))
                    .collect(Collectors.joining(","))))
            .await();
    }

    // ---------------------------------------------------------------------------------------------


    @RequiredArgsConstructor
    private static class CollectorThread extends Thread {
        private final CloseableIterator<Row> it;
        private final List<Row> rows;
        private final Callable<Boolean> breakCondition;

        @Override
        public void run() {
            try {
                while (it.hasNext()) {
                    rows.add(it.next());

                    if (breakCondition.call() || Thread.interrupted()) {
                        break;
                    }
                }
            } catch (Exception ignored) {
            }
        }
    }
}

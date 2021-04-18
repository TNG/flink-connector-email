package com.github.airblader.imap.testing;

import static org.assertj.core.api.Assertions.fail;

import com.github.airblader.imap.table.ImapSourceOptions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import com.icegreen.greenmail.junit4.GreenMailRule;
import com.icegreen.greenmail.util.ServerSetup;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import jakarta.mail.Session;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.var;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ImapTestBase {
  // Use 4xxx instead of the default 3xxx to avoid interference with a running standalone image
  public static final ServerSetup IMAP = new ServerSetup(4143, null, "imap");
  public static final ServerSetup IMAPS = new ServerSetup(4993, null, "imaps");
  public static final ServerSetup SMTP = new ServerSetup(4025, null, "smtp");

  public static Duration DEFAULT_DURATION = Duration.ofSeconds(10);

  @Rule
  public final GreenMailRule greenMailRule =
      new GreenMailRule(new ServerSetup[] {IMAP, IMAPS, SMTP})
          .withConfiguration(
              GreenMailConfiguration.aConfig()
                  .withUser("jon@acme.org", "jon", "1234")
                  .withUser("jane@acme.org", "jane", "1234"));

  public Session session;
  public StreamExecutionEnvironment executionEnv;
  public StreamTableEnvironment tEnv;

  @Before
  public void before() throws Exception {
    greenMailRule.purgeEmailFromAllMailboxes();
    session = greenMailRule.getImap().createSession();

    executionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    executionEnv.setParallelism(1);
    executionEnv.enableCheckpointing(100L, CheckpointingMode.EXACTLY_ONCE);

    tEnv = StreamTableEnvironment.create(executionEnv);
  }

  public static MiniClusterWithClientResource getCluster() {
    return new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(1)
            .setNumberTaskManagers(1)
            .build());
  }

  public void createImapTable(String name, String schema) {
    createImapTable(name, schema, options -> {});
  }

  public void createImapTable(
      String name,
      String schema,
      Consumer<ImapSourceOptions.ImapSourceOptionsBuilder<?, ?>> optionsProvider) {
    // For weird magical reasons we cannot use Lombok's var here
    ImapSourceOptions.ImapSourceOptionsBuilder<?, ?> options =
        ImapSourceOptions.builder()
            .host("localhost")
            .port(IMAP.getPort())
            .user("jon")
            .password("1234")
            .ssl(false);
    optionsProvider.accept(options);

    var properties =
        ImmutableMap.<String, String>builder()
            .putAll(options.build().toProperties())
            .put("connector", "imap")
            .build();
    var writtenOptions =
        properties.entrySet().stream()
            .map(
                optionEntry ->
                    String.format("'%s' = '%s'", optionEntry.getKey(), optionEntry.getValue()))
            .collect(Collectors.joining(", "));

    tEnv.executeSql(String.format("CREATE TABLE %s (%s) WITH (%s)", name, schema, writtenOptions));
  }

  public List<Row> collectRowUpdates(TableResult tableResult, int expectedRowUpdates)
      throws Exception {
    return collectRowUpdates(tableResult, expectedRowUpdates, DEFAULT_DURATION);
  }

  /**
   * Collects at most {@param expectedRowUpdates} rows, with a timeout of {@param maxTime}.
   *
   * <p>If the given number of rows have been collected, it stops collecting, which means unintended
   * further rows will not be collected. If fewer than expected rows are returned, it fails the
   * test.
   */
  public List<Row> collectRowUpdates(
      TableResult tableResult, int expectedRowUpdates, Duration maxTime) throws Exception {
    var rows = new ArrayList<Row>();
    try (var it = tableResult.collect()) {
      getCollectorThread(it, rows, () -> rows.size() >= expectedRowUpdates).start();

      var watch = Stopwatch.createStarted();
      while (rows.size() < expectedRowUpdates
          && watch.elapsed(TimeUnit.MILLISECONDS) < maxTime.toMillis()) {
        Thread.sleep(50L);
      }
    }

    if (rows.size() < expectedRowUpdates) {
      fail(
          "%s ms have passed, but only %d of expected %d rows have been returned",
          maxTime.toMillis(), rows.size(), expectedRowUpdates);
    }

    return rows;
  }

  /**
   * Waits for the in {@param maxTime} specified amount of time while collecting results before
   * returning them.
   */
  public List<Row> collectRowUpdates(TableResult tableResult, Duration maxTime) throws Exception {
    var rows = new ArrayList<Row>();
    try (var it = tableResult.collect()) {
      getCollectorThread(it, rows, () -> false).start();
      Thread.sleep(maxTime.toMillis());
    }

    return rows;
  }

  private Thread getCollectorThread(
      CloseableIterator<Row> it, List<Row> rows, Supplier<Boolean> breakCondition) {
    return new Thread(
        () -> {
          try {
            while (it.hasNext()) {
              rows.add(it.next());

              if (breakCondition.get()) {
                break;
              }
            }
          } catch (Exception ignored) {
          }
        });
  }
}

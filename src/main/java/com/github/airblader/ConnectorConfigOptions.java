package com.github.airblader;

import java.time.Duration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

class ConnectorConfigOptions {
  public static final ConfigOption<String> HOST =
      ConfigOptions.key("host")
          .stringType()
          .noDefaultValue()
          .withDescription("Host for the IMAP server");
  public static final ConfigOption<Integer> PORT =
      ConfigOptions.key("port")
          .intType()
          .noDefaultValue()
          .withDescription("Port for the IMAP server");
  public static final ConfigOption<String> USER =
      ConfigOptions.key("user")
          .stringType()
          .noDefaultValue()
          .withDescription("Username to authenticate with");
  public static final ConfigOption<String> PASSWORD =
      ConfigOptions.key("password")
          .stringType()
          .noDefaultValue()
          .withDescription("Password to authenticate with");
  public static final ConfigOption<Boolean> SSL =
      ConfigOptions.key("ssl").booleanType().defaultValue(true).withDescription("Use SSL");
  public static final ConfigOption<String> FOLDER =
      ConfigOptions.key("folder")
          .stringType()
          .defaultValue("Inbox")
          .withDescription("Folder for which to list messages");
  public static final ConfigOption<String> MODE =
      ConfigOptions.key("scan.startup.mode")
          .stringType()
          .defaultValue("latest")
          .withDescription(
              "'all' = Initially fetch all emails in the folder, 'latest' = Only observe changes in the folder");
  public static final ConfigOption<Boolean> IDLE =
      ConfigOptions.key("scan.idle")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Use IDLE instead of polling (on by default and automatically falls back to polling)");
  public static final ConfigOption<Duration> INTERVAL =
      ConfigOptions.key("scan.interval")
          .durationType()
          .defaultValue(Duration.ofSeconds(1L))
          .withDescription(
              "Duration between polling attempts (only used if IDLE is disabled or unavailable)");
  public static final ConfigOption<Boolean> DELETIONS =
      ConfigOptions.key("scan.deletions")
          .booleanType()
          .defaultValue(false)
          .withDescription("Remove deleted emails");

  private ConnectorConfigOptions() {}
}

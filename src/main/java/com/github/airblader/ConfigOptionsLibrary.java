package com.github.airblader;

import com.github.airblader.imap.AddressFormat;
import java.time.Duration;
import java.util.Set;
import org.apache.commons.compress.utils.Sets;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class ConfigOptionsLibrary {
  public static final ConfigOption<String> HOST =
      ConfigOptions.key("host")
          .stringType()
          .noDefaultValue()
          .withDescription("Host for the IMAP server");
  public static final ConfigOption<String> ENV_HOST =
      ConfigOptions.key("host.env")
          .stringType()
          .noDefaultValue()
          .withDescription("Environment variable to use to get the hostname (this overrides host)");
  public static final ConfigOption<Integer> PORT =
      ConfigOptions.key("port")
          .intType()
          .noDefaultValue()
          .withDescription("Port for the IMAP server");
  public static final ConfigOption<String> ENV_PORT =
      ConfigOptions.key("port.env")
          .stringType()
          .noDefaultValue()
          .withDescription("Environment variable to use to get the port (this overrides port)");
  public static final ConfigOption<String> USER =
      ConfigOptions.key("user")
          .stringType()
          .noDefaultValue()
          .withDescription("Username to authenticate with");
  public static final ConfigOption<String> ENV_USER =
      ConfigOptions.key("user.env")
          .stringType()
          .noDefaultValue()
          .withDescription("Environment variable to use to get the username (this overrides user)");
  public static final ConfigOption<String> PASSWORD =
      ConfigOptions.key("password")
          .stringType()
          .noDefaultValue()
          .withDescription("Password to authenticate with");
  public static final ConfigOption<String> ENV_PASSWORD =
      ConfigOptions.key("password.env")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Environment variable to use to get the password (this overrides password)");
  public static final ConfigOption<Boolean> SSL =
      ConfigOptions.key("ssl").booleanType().defaultValue(true).withDescription("Use SSL");
  public static final ConfigOption<String> FOLDER =
      ConfigOptions.key("scan.folder")
          .stringType()
          .defaultValue("Inbox")
          .withDescription("Folder for which to list messages");
  public static final ConfigOption<String> MODE =
      ConfigOptions.key("scan.startup.mode")
          .stringType()
          .defaultValue("latest")
          .withDescription(
              "'all' = Initially fetch all emails in the folder, 'latest' = Only observe changes in the folder");
  public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
      ConfigOptions.key("scan.startup.timeout")
          .durationType()
          .defaultValue(Duration.ofSeconds(60))
          .withDescription("Socket connection timeout");
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
  public static final ConfigOption<Boolean> HEARTBEAT =
      ConfigOptions.key("scan.idle.heartbeat")
          .booleanType()
          .defaultValue(true)
          .withDescription("Emit a periodic heartbeat when using idle");
  public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
      ConfigOptions.key("scan.idle.heartbeat.interval")
          .durationType()
          .defaultValue(Duration.ofMinutes(15))
          .withDescription("Frequency of the idle heartbeat");
  public static final ConfigOption<AddressFormat> ADDRESS_FORMAT =
      ConfigOptions.key("scan.format.address")
          .enumType(AddressFormat.class)
          .defaultValue(AddressFormat.DEFAULT)
          .withDescription("'default' = full address, 'simple' = only email address");

  public static final Set<ConfigOption<?>> CATALOG_OPTIONS =
      Sets.newHashSet(
          HOST,
          ENV_HOST,
          PORT,
          ENV_PORT,
          USER,
          ENV_USER,
          PASSWORD,
          ENV_PASSWORD,
          SSL,
          CONNECTION_TIMEOUT);

  public static final Set<ConfigOption<?>> TABLE_OPTIONS =
      Sets.newHashSet(
          HOST,
          ENV_HOST,
          PORT,
          ENV_PORT,
          USER,
          ENV_USER,
          PASSWORD,
          ENV_PASSWORD,
          SSL,
          MODE,
          CONNECTION_TIMEOUT,
          IDLE,
          INTERVAL,
          DELETIONS,
          HEARTBEAT,
          HEARTBEAT_INTERVAL,
          ADDRESS_FORMAT,
          FOLDER);

  private ConfigOptionsLibrary() {}
}

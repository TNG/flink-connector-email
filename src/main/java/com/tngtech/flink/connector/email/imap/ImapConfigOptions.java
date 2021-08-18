package com.tngtech.flink.connector.email.imap;

import com.tngtech.flink.connector.email.common.EmailConfigOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

@UtilityClass
@PublicEvolving
public class ImapConfigOptions {

    public static final ConfigOption<String> HOST = EmailConfigOptions.HOST;

    public static final ConfigOption<Long> PORT = EmailConfigOptions.PORT;

    public static final ConfigOption<String> USER = EmailConfigOptions.USER;

    public static final ConfigOption<String> PASSWORD = EmailConfigOptions.PASSWORD;

    public static final ConfigOption<Boolean> SSL = EmailConfigOptions.SSL;

    public static final ConfigOption<String> FORMAT = EmailConfigOptions.FORMAT;

    public static final ConfigOption<String> FOLDER = ConfigOptions.key("folder")
        .stringType()
        .defaultValue("INBOX");

    // ---------------------------------------------------------------------------------------------

    public static final ConfigOption<StartupMode> MODE = ConfigOptions.key("mode")
        .enumType(StartupMode.class)
        .defaultValue(StartupMode.ALL)
        .withDescription("Defines which emails to read from the specified folder.");

    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("batch-size")
        .intType()
        .defaultValue(50)
        .withDescription(Description.builder()
            .text("Number of messages to fetch per batch.")
            .linebreak()
            .text(String.format("Only used if '%s' = '%s'.", MODE.key(), StartupMode.ALL))
            .build());

    public static final ConfigOption<Long> OFFSET = ConfigOptions.key("offset")
        .longType()
        .noDefaultValue()
        .withDescription(Description.builder()
            .text("If specified, this is the UID from which on messages will be fetched.")
            .linebreak()
            .text(String.format("Only used if '%s' = '%s'.", MODE.key(), StartupMode.ALL))
            .build());

    // ---------------------------------------------------------------------------------------------

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
        ConfigOptions.key("connection.timeout")
            .durationType()
            .defaultValue(Duration.ofMinutes(1L));

    public static final ConfigOption<Duration> INTERVAL = ConfigOptions.key("interval")
        .durationType()
        .defaultValue(Duration.ofSeconds(1L))
        .withDescription(Description.builder()
            .text("Time between polling attempts.")
            .linebreak()
            .text(
                "This interval is only used if the server doesn't support IDLE. In this case, the connector falls back to polling.")
            .build());

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
        ConfigOptions.key("heartbeat.interval")
            .durationType()
            .defaultValue(Duration.ofMinutes(15L))
            .withDescription(
                "Frequency with which to send a heartbeat signal to maintain the IDLE connection.");

    // ---------------------------------------------------------------------------------------------


    @RequiredArgsConstructor
    public enum StartupMode {
        ALL("all"),
        NEW("new"),
        CURRENT("current");

        @Getter
        private final String value;

        public boolean isOneOf(StartupMode... modes) {
            for (StartupMode mode : modes) {
                if (this == mode) {
                    return true;
                }
            }

            return false;
        }
    }

}

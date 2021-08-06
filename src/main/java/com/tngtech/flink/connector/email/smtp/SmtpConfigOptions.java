package com.tngtech.flink.connector.email.smtp;

import com.tngtech.flink.connector.email.common.EmailConfigOptions;
import lombok.experimental.UtilityClass;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

@UtilityClass
@PublicEvolving
public class SmtpConfigOptions {

    public static final ConfigOption<String> HOST = EmailConfigOptions.HOST;

    public static final ConfigOption<Long> PORT = EmailConfigOptions.PORT;

    public static final ConfigOption<String> USER = EmailConfigOptions.USER;

    public static final ConfigOption<String> PASSWORD = EmailConfigOptions.PASSWORD;

    public static final ConfigOption<Boolean> SSL = EmailConfigOptions.SSL;

    public static final ConfigOption<String> FORMAT = EmailConfigOptions.FORMAT;

}

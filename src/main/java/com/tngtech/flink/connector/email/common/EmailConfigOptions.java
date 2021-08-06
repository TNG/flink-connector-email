package com.tngtech.flink.connector.email.common;

import lombok.experimental.UtilityClass;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@UtilityClass
@Internal
public class EmailConfigOptions {

    public static final ConfigOption<String> HOST = ConfigOptions.key("host")
        .stringType()
        .noDefaultValue();

    public static final ConfigOption<Long> PORT = ConfigOptions.key("port")
        .longType()
        .noDefaultValue();

    public static final ConfigOption<String> USER = ConfigOptions.key("user")
        .stringType()
        .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
        .stringType()
        .noDefaultValue();

    public static final ConfigOption<Boolean> SSL = ConfigOptions.key("ssl")
        .booleanType()
        .defaultValue(false);

    public static final ConfigOption<String> FORMAT = ConfigOptions.key("format")
        .stringType()
        .defaultValue("raw");

}

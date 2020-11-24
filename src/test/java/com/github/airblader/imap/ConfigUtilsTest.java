package com.github.airblader.imap;

import static com.github.airblader.ConfigUtils.getEffectiveProperty;
import static com.github.airblader.ConfigUtils.validateOptionOrEnv;
import static org.assertj.core.api.Assertions.*;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ConfigUtilsTest {
  @Test
  @DataProvider(
      value = {"null | null | false", "foo | null | true", "null | foo | true", "foo | foo | true"},
      splitBy = "\\|")
  public void testValidateOptionOrEnv(
      String propValue, String envPropValue, boolean expectSuccess) {
    var option = ConfigOptions.key("prop").stringType().noDefaultValue();
    var envOption = ConfigOptions.key("envProp").stringType().noDefaultValue();

    var config = new Configuration();
    if (propValue != null) {
      config.setString(option, propValue);
    }
    if (envPropValue != null) {
      config.setString(envOption, envPropValue);
    }

    ThrowableAssert.ThrowingCallable fn = () -> validateOptionOrEnv(config, option, envOption);
    if (expectSuccess) {
      assertThatCode(fn).doesNotThrowAnyException();
    } else {
      assertThatThrownBy(fn).isInstanceOf(ValidationException.class);
    }
  }

  @Test
  @DataProvider(
      value = {
        "EXISTING_HOST | null | envHost",
        "EXISTING_HOST | localhost | envHost",
        "null | localhost | localhost",
        "null | null | null"
      },
      splitBy = "\\|")
  public void testGetEffectiveProperty(String envProperty, String property, String expected) {
    System.setProperty("EXISTING_HOST", "envHost");
    assertThat(getEffectiveProperty(envProperty, property)).isEqualTo(expected);
  }

  @Test
  @DataProvider(
      value = {
        "EXISTING_PORT | null | 1234",
        "EXISTING_PORT | 42 | 1234",
        "null | 42 | 42",
        "null | null | null"
      },
      splitBy = "\\|")
  public void testGetEffectivePropertyWithParser(
      String envProperty, Integer property, Integer expected) {
    System.setProperty("EXISTING_PORT", "1234");
    assertThat(getEffectiveProperty(envProperty, property, Integer::parseInt)).isEqualTo(expected);
  }
}

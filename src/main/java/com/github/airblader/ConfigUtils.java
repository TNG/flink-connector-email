package com.github.airblader;

import java.util.function.Function;
import lombok.experimental.UtilityClass;
import lombok.var;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;

@UtilityClass
public class ConfigUtils {
  public static <T> void validateOptionOrEnv(
      FactoryUtil.TableFactoryHelper factoryHelper,
      ConfigOption<T> option,
      ConfigOption<String> envOption) {
    var value = factoryHelper.getOptions().get(option);
    var envValue = factoryHelper.getOptions().get(envOption);
    if (value == null && envValue == null) {
      throw new ValidationException(
          String.format("One of '%s' or '%s' must be set.", option.key(), envOption.key()));
    }
  }

  public static String getEffectiveProperty(String envProperty, String property) {
    return getEffectiveProperty(envProperty, property, Function.identity());
  }

  public static <T> T getEffectiveProperty(
      String envProperty, T property, Function<String, T> parser) {
    if (envProperty != null) {
      var envValue = System.getProperty(envProperty);
      if (envValue != null) {
        return parser.apply(envValue);
      }
    }

    return property;
  }
}

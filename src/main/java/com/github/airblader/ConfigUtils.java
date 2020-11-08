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

  public static String getValueWithEnvOverride(
      FactoryUtil.TableFactoryHelper factoryHelper,
      ConfigOption<String> envOption,
      ConfigOption<String> option) {
    return getValueWithEnvOverride(factoryHelper, envOption, option, Function.identity());
  }

  public static <T> T getValueWithEnvOverride(
      FactoryUtil.TableFactoryHelper factoryHelper,
      ConfigOption<String> envOption,
      ConfigOption<T> option,
      Function<String, T> parserFn) {
    var envName = factoryHelper.getOptions().get(envOption);
    if (envName != null) {
      var envValue = System.getProperty(envName);
      if (envValue != null) {
        return parserFn.apply(envValue);
      }
    }

    return factoryHelper.getOptions().get(option);
  }
}

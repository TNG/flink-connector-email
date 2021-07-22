package com.github.airblader.imap.catalog;

import static com.github.airblader.ConfigOptionsLibrary.*;

import java.util.HashSet;
import java.util.Set;
import lombok.var;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

public class ImapCatalogFactory implements CatalogFactory {

  @Override
  public String factoryIdentifier() {
    return "imap";
  }

  @Override
  public Catalog createCatalog(Context context) {
    var factoryHelper = FactoryUtil.createCatalogFactoryHelper(this, context);
    var options = factoryHelper.getOptions();

    return new ImapCatalog(
        context.getName(),
        ImapCatalogOptions.builder()
            .host(options.get(HOST))
            .envHost(options.get(ENV_HOST))
            .port(options.get(PORT))
            .envPort(options.get(ENV_PORT))
            .user(options.get(USER))
            .envUser(options.get(ENV_USER))
            .password(options.get(PASSWORD))
            .envPassword(options.get(ENV_PASSWORD))
            .ssl(options.get(SSL))
            .connectionTimeout(options.get(CONNECTION_TIMEOUT))
            .build());
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return new HashSet<>();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return CATALOG_OPTIONS;
  }
}

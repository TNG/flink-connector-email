package com.github.airblader.imap;

import com.github.airblader.imap.catalog.ImapCatalogOptions;
import java.util.Properties;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ImapUtils {

  public static Properties getImapProperties(ImapCatalogOptions catalogOptions) {
    Properties props = new Properties();
    props.put("mail.store.protocol", "imap");
    props.put("mail.imap.ssl.enable", catalogOptions.isSsl());
    props.put("mail.imap.starttls.enable", true);
    props.put("mail.imap.auth", true);
    props.put("mail.imap.host", catalogOptions.getEffectiveHost());
    if (catalogOptions.getEffectivePort() != null) {
      props.put("mail.imap.port", catalogOptions.getEffectivePort());
    }

    props.put("mail.imap.connectiontimeout", catalogOptions.getConnectionTimeout().toMillis());
    props.put("mail.imap.partialfetch", false);
    props.put("mail.imap.peek", true);
    return props;
  }
}

package com.github.airblader.imap.table;

import com.github.airblader.imap.AddressFormat;
import com.github.airblader.imap.ScanMode;
import com.github.airblader.imap.catalog.ImapCatalogOptions;
import java.io.Serializable;
import java.time.Duration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class ImapSourceOptions extends ImapCatalogOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String folder;
  private final ScanMode mode;
  private final boolean idle;
  private final boolean heartbeat;
  private final Duration heartbeatInterval;
  private final Duration interval;
  private final boolean deletions;
  private final AddressFormat addressFormat;
}

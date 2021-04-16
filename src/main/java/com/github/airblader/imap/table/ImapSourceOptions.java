package com.github.airblader.imap.table;

import static com.github.airblader.ConfigOptionsLibrary.*;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

import com.github.airblader.imap.AddressFormat;
import com.github.airblader.imap.ScanMode;
import com.github.airblader.imap.catalog.ImapCatalogOptions;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import lombok.var;

@Data
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class ImapSourceOptions extends ImapCatalogOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String folder;
  private final ScanMode mode;
  private final Long scanFromUID;
  private final int batchSize;
  private final boolean idle;
  private final boolean heartbeat;
  private final Duration heartbeatInterval;
  private final Duration interval;
  private final boolean deletions;
  private final AddressFormat addressFormat;

  @Override
  public Map<String, String> toProperties() {
    var properties = super.toProperties();
    if (folder != null) {
      properties.put(FOLDER.key(), folder);
    }
    if (mode != null) {
      properties.put(MODE.key(), mode.getValue());
    }
    if (scanFromUID != null) {
      properties.put(SCAN_FROM_UID.key(), String.valueOf(scanFromUID));
    }
    properties.put(BATCH_SIZE.key(), String.valueOf(batchSize));
    properties.put(IDLE.key(), String.valueOf(idle));
    properties.put(HEARTBEAT.key(), String.valueOf(heartbeat));
    if (heartbeatInterval != null) {
      properties.put(HEARTBEAT_INTERVAL.key(), formatWithHighestUnit(heartbeatInterval));
    }
    if (interval != null) {
      properties.put(INTERVAL.key(), formatWithHighestUnit(interval));
    }
    properties.put(DELETIONS.key(), String.valueOf(deletions));
    if (addressFormat != null) {
      properties.put(ADDRESS_FORMAT.key(), addressFormat.getValue());
    }

    return properties;
  }
}

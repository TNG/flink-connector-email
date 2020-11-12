package com.github.airblader.imap;

import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class ImapSourceOptions extends ImapCatalogOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String folder;
}

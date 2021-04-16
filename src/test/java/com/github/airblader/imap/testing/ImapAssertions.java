package com.github.airblader.imap.testing;

import org.apache.flink.types.Row;
import org.assertj.core.api.Assertions;

public class ImapAssertions extends Assertions {
  public static RowAssert assertThat(Row row) {
    return new RowAssert(row, RowAssert.class);
  }
}

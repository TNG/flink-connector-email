package com.github.airblader.imap.testing;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.types.Row;
import org.assertj.core.api.AbstractAssert;

public class RowAssert extends AbstractAssert<RowAssert, Row> {
  public RowAssert(Row row, Class<?> selfType) {
    super(row, selfType);
  }

  public RowAssert isEqualTo(Object value) {
    return isEqualTo(new Object[] {value});
  }

  public RowAssert isEqualTo(Object... values) {
    if (values.length != actual.getArity()) {
      throw failure("Expected arity %d, but got %d", values.length, actual.getArity());
    }

    for (int i = 0; i < actual.getArity(); i++) {
      assertThat(actual.getField(i)).isEqualTo(values[i]);
    }

    return this;
  }
}

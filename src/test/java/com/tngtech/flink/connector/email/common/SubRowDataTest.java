package com.tngtech.flink.connector.email.common;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SubRowDataTest {
    @Test
    public void test() {
        final RowData row = GenericRowData.of(
            StringData.fromString("a"),
            StringData.fromString("b"),
            StringData.fromString("c"),
            StringData.fromString("d"),
            StringData.fromString("e"));

        final RowData subRow1 = new SubRowData(row, 0, 2);
        assertThat(subRow1.getArity()).isEqualTo(2);
        assertThat(subRow1.getString(0).toString()).isEqualTo("a");
        assertThat(subRow1.getString(1).toString()).isEqualTo("b");

        final RowData subRow2 = new SubRowData(row, 3, 4);
        assertThat(subRow2.getArity()).isEqualTo(1);
        assertThat(subRow2.getString(0).toString()).isEqualTo("d");

        final RowData subRow3 = new SubRowData(row, 4, 4);
        assertThat(subRow3.getArity()).isEqualTo(0);
    }
}

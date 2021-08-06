package com.tngtech.flink.connector.email.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;

@Internal
public class SubRowData implements RowData {

    private final RowData rowData;
    private final int startIndex;
    private final int endIndex;

    public SubRowData(RowData rowData, int startIndex, int endIndex) {
        this.rowData = rowData;
        this.startIndex = startIndex;
        this.endIndex = endIndex;

        if (startIndex < 0 || startIndex >= rowData.getArity()) {
            throw new IllegalArgumentException("startIndex must be within bounds.");
        }

        if (endIndex < 0 || endIndex > rowData.getArity() || endIndex < startIndex) {
            throw new IllegalArgumentException("endIndex must be within bounds.");
        }
    }

    private int convertPos(int pos) {
        assert pos + startIndex < endIndex;
        return pos + startIndex;
    }

    @Override
    public int getArity() {
        return endIndex - startIndex;
    }

    @Override
    public RowKind getRowKind() {
        return rowData.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        rowData.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return rowData.isNullAt(convertPos(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return rowData.getBoolean(convertPos(pos));
    }

    @Override
    public byte getByte(int pos) {
        return rowData.getByte(convertPos(pos));
    }

    @Override
    public short getShort(int pos) {
        return rowData.getShort(convertPos(pos));
    }

    @Override
    public int getInt(int pos) {
        return rowData.getInt(convertPos(pos));
    }

    @Override
    public long getLong(int pos) {
        return rowData.getLong(convertPos(pos));
    }

    @Override
    public float getFloat(int pos) {
        return rowData.getFloat(convertPos(pos));
    }

    @Override
    public double getDouble(int pos) {
        return rowData.getDouble(convertPos(pos));
    }

    @Override
    public StringData getString(int pos) {
        return rowData.getString(convertPos(pos));
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return rowData.getDecimal(convertPos(pos), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return rowData.getTimestamp(convertPos(pos), precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return rowData.getRawValue(convertPos(pos));
    }

    @Override
    public byte[] getBinary(int pos) {
        return rowData.getBinary(convertPos(pos));
    }

    @Override
    public ArrayData getArray(int pos) {
        return rowData.getArray(convertPos(pos));
    }

    @Override
    public MapData getMap(int pos) {
        return rowData.getMap(convertPos(pos));
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return rowData.getRow(convertPos(pos), numFields);
    }
}

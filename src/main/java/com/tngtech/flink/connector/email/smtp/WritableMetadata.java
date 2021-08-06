package com.tngtech.flink.connector.email.smtp;

import jakarta.mail.Address;
import jakarta.mail.Message.RecipientType;
import jakarta.mail.internet.MimeMessage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

import static com.tngtech.flink.connector.email.common.MessageUtil.decodeAddresses;

@PublicEvolving
@RequiredArgsConstructor
public enum WritableMetadata {
    SUBJECT(
        "subject",
        DataTypes.STRING().nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(RowData rowData, int pos, MimeMessage message) throws Exception {
                final String subject = rowData.getString(pos).toString();
                message.setSubject(subject);
            }
        }
    ),

    FROM(
        "from",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(RowData rowData, int pos, MimeMessage message) throws Exception {
                final Address[] addresses = decodeAddresses(rowData.getArray(pos));
                message.addFrom(addresses);
            }
        }
    ),

    TO(
        "to",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(RowData rowData, int pos, MimeMessage message) throws Exception {
                final Address[] addresses = decodeAddresses(rowData.getArray(pos));
                message.addRecipients(RecipientType.TO, addresses);
            }
        }
    ),

    CC(
        "cc",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(RowData rowData, int pos, MimeMessage message) throws Exception {
                final Address[] addresses = decodeAddresses(rowData.getArray(pos));
                message.addRecipients(RecipientType.CC, addresses);
            }
        }
    ),

    BCC(
        "bcc",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(RowData rowData, int pos, MimeMessage message) throws Exception {
                final Address[] addresses = decodeAddresses(rowData.getArray(pos));
                message.addRecipients(RecipientType.BCC, addresses);
            }
        }
    ),

    REPLY_TO(
        "replyTo",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(RowData rowData, int pos, MimeMessage message) throws Exception {
                final Address[] addresses = decodeAddresses(rowData.getArray(pos));
                message.setReplyTo(addresses);
            }
        }
    );

    @Getter
    private final String key;

    @Getter
    private final DataType type;

    @Getter
    private final Converter converter;

    public static WritableMetadata ofKey(String key) {
        for (WritableMetadata candidate : values()) {
            if (key.equals(candidate.getKey())) {
                return candidate;
            }
        }

        throw new IllegalArgumentException(String.format("Metadata key '%s' not found.", key));
    }

    // ---------------------------------------------------------------------------------------------


    @FunctionalInterface
    interface Converter extends Serializable {
        void convert(RowData rowData, int pos, MimeMessage message) throws Exception;
    }
}

package com.tngtech.flink.connector.email.imap;

import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.Flags;
import jakarta.mail.Message;
import jakarta.mail.Message.RecipientType;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

import static com.tngtech.flink.connector.email.common.MessageUtil.*;

@PublicEvolving
@RequiredArgsConstructor
public enum ReadableMetadata {
    UID(
        "uid",
        DataTypes.BIGINT().notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getFolder().getUID(context.getMessage());
            }
        }),

    SUBJECT(
        "subject",
        DataTypes.STRING().nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getSubject();
            }
        }),

    SENT(
        "sent",
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getSentDate().toInstant();
            }
        }),

    RECEIVED(
        "received",
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getReceivedDate().toInstant();
            }
        }),

    CONTENT_TYPE(
        "contentType",
        DataTypes.STRING().nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getContentType();
            }
        }
    ),

    SIZE(
        "sizeInBytes",
        DataTypes.INT().notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getSize();
            }
        }
    ),

    SEEN(
        "seen",
        DataTypes.BOOLEAN().notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getFlags().contains(Flags.Flag.SEEN);
            }
        }
    ),

    DRAFT(
        "draft",
        DataTypes.BOOLEAN().notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getFlags().contains(Flags.Flag.DRAFT);
            }
        }
    ),

    ANSWERED(
        "answered",
        DataTypes.BOOLEAN().notNull(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return context.getMessage().getFlags().contains(Flags.Flag.ANSWERED);
            }
        }
    ),

    FROM(
        "from",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeAddresses(context.getMessage().getFrom());
            }
        }),

    FROM_FIRST(
        "from.first",
        DataTypes.STRING().nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeFirstAddress(context.getMessage().getFrom());
            }
        }),

    TO(
        "to",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeAddresses(context.getMessage().getRecipients(RecipientType.TO));
            }
        }),

    TO_FIRST(
        "to.first",
        DataTypes.STRING().nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeFirstAddress(context.getMessage().getRecipients(RecipientType.TO));
            }
        }),

    CC(
        "cc",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeAddresses(context.getMessage().getRecipients(RecipientType.CC));
            }
        }),

    BCC(
        "bcc",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeAddresses(context.getMessage().getRecipients(RecipientType.BCC));
            }
        }),

    RECIPIENTS(
        "recipients",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeAddresses(context.getMessage().getAllRecipients());
            }
        }),

    REPLY_TO(
        "replyTo",
        DataTypes.ARRAY(DataTypes.STRING()).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeAddresses(context.getMessage().getReplyTo());
            }
        }),

    HEADERS(
        "headers",
        DataTypes.ARRAY(DataTypes.ROW(
            DataTypes.FIELD("key", DataTypes.STRING()),
            DataTypes.FIELD("value", DataTypes.STRING())
        )).nullable(),
        new Converter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(Context context) throws Exception {
                return encodeHeaders(context.getMessage().getAllHeaders());
            }
        });

    @Getter
    private final String key;

    @Getter
    private final DataType type;

    @Getter
    private final Converter converter;

    public static ReadableMetadata ofKey(String key) {
        for (ReadableMetadata candidate : values()) {
            if (key.equals(candidate.getKey())) {
                return candidate;
            }
        }

        throw new IllegalArgumentException(String.format("Metadata key '%s' not found.", key));
    }

    // ---------------------------------------------------------------------------------------------


    public interface Context {
        IMAPFolder getFolder();

        Message getMessage();

        static Context of(IMAPFolder folder, Message message) {
            return new DefaultContext(folder, message);
        }

        @Data
        class DefaultContext implements Context {
            private final IMAPFolder folder;
            private final Message message;
        }
    }


    @FunctionalInterface
    interface Converter extends Serializable {
        Object convert(Context context) throws Exception;
    }
}

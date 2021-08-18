package com.tngtech.flink.connector.email.smtp;

import com.icegreen.greenmail.util.GreenMailUtil;
import com.tngtech.flink.connector.email.testing.TestBase;
import jakarta.mail.internet.MimeMessage;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;

public class SmtpSinkTest extends TestBase {

    @Test
    public void simpleInsert() throws Exception {
        final ResolvedSchema schema = ResolvedSchema.of(
            metadataColumn(WritableMetadata.SUBJECT),
            metadataColumn(WritableMetadata.FROM),
            metadataColumn(WritableMetadata.TO),
            Column.physical("content", DataTypes.STRING())
        );

        createSmtpSink("T", schema);

        tEnv.fromValues(schema.toSinkRowDataType(),
            row(
                "Subject",
                new String[] {"sender@tngtech.test"},
                new String[] {"ingo@tngtech.test"},
                "Message Content"
            )
        ).executeInsert("T").await();

        final MimeMessage[] sentMessages = greenMailRule.getReceivedMessages();
        assertThat(sentMessages).hasSize(1);
        assertThat(sentMessages[0].getSubject()).isEqualTo("Subject");
        assertThat(GreenMailUtil.getBody(sentMessages[0])).contains("Message Content");
    }
}

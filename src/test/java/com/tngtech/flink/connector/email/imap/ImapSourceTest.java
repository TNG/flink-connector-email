package com.tngtech.flink.connector.email.imap;

import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import com.tngtech.flink.connector.email.testing.TestBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.icegreen.greenmail.util.GreenMailUtil.sendTextEmail;
import static org.assertj.core.api.Assertions.assertThat;

public class ImapSourceTest extends TestBase {

    private static final Tuple2<String, String> LOGIN = Tuple2.of("ingo@tngtech.com", "123");

    @Override
    public GreenMailConfiguration getGreenmailConfiguration() {
        return GreenMailConfiguration.aConfig().withUser(LOGIN.f0, LOGIN.f1);
    }

    @Test
    public void simpleSelect() throws Exception {
        final ResolvedSchema schema = ResolvedSchema.of(
            metadataColumn(ReadableMetadata.SUBJECT),
            metadataColumn(ReadableMetadata.FROM_FIRST),
            metadataColumn(ReadableMetadata.TO_FIRST),
            Column.physical("content", DataTypes.STRING())
        );

        sendTextEmail("ingo@tngtech.com", "sender@tngtech.com", "Subject", "Message Content", SMTP);

        final List<Row> rows = collect(createImapSource("T", schema, getLogin()).execute(), 1);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo("Subject");
        assertThat(rows.get(0).getField(1)).isEqualTo("sender@tngtech.com");
        assertThat(rows.get(0).getField(2)).isEqualTo("ingo@tngtech.com");
        assertThat(rows.get(0).getField(3)).isEqualTo("Message Content");
    }

    // ---------------------------------------------------------------------------------------------

    private static Map<String, String> getLogin() {
        final Map<String, String> options = new HashMap<>();
        options.put("user", LOGIN.f0);
        options.put("password", LOGIN.f1);
        return options;
    }
}

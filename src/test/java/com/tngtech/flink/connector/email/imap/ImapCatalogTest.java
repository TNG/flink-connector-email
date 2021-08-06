package com.tngtech.flink.connector.email.imap;

import com.tngtech.flink.connector.email.testing.TestBase;
import org.apache.flink.table.catalog.Catalog;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ImapCatalogTest extends TestBase {

    @Test
    public void testListTables() throws Exception {
        final Catalog catalog = createImapCatalog("local");
        assertThat(catalog.listTables(catalog.getDefaultDatabase())).containsExactly("INBOX");
    }
}

package com.github.airblader.imap.table;

import static com.github.airblader.imap.testing.ImapAssertions.assertThat;
import static com.icegreen.greenmail.util.GreenMailUtil.sendTextEmail;

import com.github.airblader.imap.ScanMode;
import com.github.airblader.imap.testing.ImapTestBase;
import lombok.var;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class ImapSourceFunctionTest extends ImapTestBase {
  @ClassRule public static MiniClusterWithClientResource flinkCluster = getCluster();

  @Test
  public void collectSingleExistingMessage() throws Exception {
    createImapTable(
        "inbox", "subject STRING, content STRING", options -> options.mode(ScanMode.ALL));
    sendTextEmail("jon@acme.org", "jane@acme.org", "Subject 123", "Message 123", SMTP);

    var tableResult = tEnv.executeSql("SELECT * FROM inbox");
    var rows = collectRows(tableResult, 1);

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).isEqualTo("Subject 123", "Message 123");
  }

  @Test
  public void foo() throws Exception {
    createImapTable("inbox", "subject STRING");
    var tableResult = tEnv.executeSql("SELECT * FROM inbox");
    Thread.sleep(500L);

    sendTextEmail("jon@acme.org", "jon@acme.org", "Test", "Test", SMTP);

    var rows = collectRows(tableResult, 1);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField(0)).isEqualTo("Test");
    // TODO Custom row matcher
  }
}

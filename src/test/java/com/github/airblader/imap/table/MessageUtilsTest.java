package com.github.airblader.imap.table;

import static com.github.airblader.imap.table.MessageUtils.getMessageContent;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.airblader.imap.testing.ImapTestBase;
import jakarta.mail.internet.MimeMessage;
import lombok.var;
import org.junit.Test;

public class MessageUtilsTest extends ImapTestBase {
  @Test
  public void testGetMessageContentPlain() throws Exception {
    var message = new MimeMessage(session);
    message.setText("Some Content");

    assertThat(getMessageContent(message)).isEqualTo("Some Content");
  }
}

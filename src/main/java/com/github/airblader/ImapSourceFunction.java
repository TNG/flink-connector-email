package com.github.airblader;

import static com.github.airblader.MessageUtils.*;

import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.*;
import jakarta.mail.event.MessageCountAdapter;
import jakarta.mail.event.MessageCountEvent;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

// TODO Catch exceptions (connect, connection closed)
// TODO Property for fetch profile as performance improvement
@RequiredArgsConstructor
public class ImapSourceFunction extends RichSourceFunction<RowData> {
  private final ConnectorOptions connectorOptions;
  private final String[] fieldNames;

  private transient boolean running = false;
  private transient Store store;
  private transient Folder folder;

  private volatile boolean supportsIdle = true;

  @Override
  public void open(Configuration parameters) {}

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    var session = Session.getInstance(getImapProperties(), null);
    store = session.getStore();
    try {
      store.connect(connectorOptions.getUser(), connectorOptions.getPassword());
    } catch (MessagingException e) {
      throw new ImapSourceException("Failed to connect to the IMAP server.", e);
    }

    try {
      folder = store.getFolder(connectorOptions.getFolder());
      folder.open(Folder.READ_ONLY);
    } catch (MessagingException e) {
      throw new ImapSourceException("Could not open folder " + folder.getName(), e);
    }

    if (!folder.exists()) {
      throw new ImapSourceException("Folder " + folder.getName() + " does not exist.");
    }

    folder.addMessageCountListener(
        new MessageCountAdapter() {
          @Override
          public void messagesAdded(MessageCountEvent event) {
            collectMessages(ctx, RowKind.INSERT, event.getMessages());
          }

          @Override
          public void messagesRemoved(MessageCountEvent event) {
            if (!connectorOptions.isDeletions()) {
              return;
            }

            collectMessages(ctx, RowKind.DELETE, event.getMessages());
          }
        });

    if (connectorOptions.getMode() == ScanMode.ALL) {
      collectMessages(ctx, RowKind.INSERT, folder.getMessages());
    }

    running = true;
    enterWaitLoop();
  }

  @Override
  public void cancel() {
    running = false;

    try {
      if (folder != null) {
        folder.close(false);
      }

      if (store != null) {
        store.close();
      }
    } catch (MessagingException ignored) {
    }
  }

  private void collectMessages(SourceContext<RowData> ctx, RowKind rowKind, Message[] messages) {
    for (Message message : messages) {
      try {
        collectMessage(ctx, rowKind, message);
      } catch (Exception ignored) {
      }
    }
  }

  private void collectMessage(SourceContext<RowData> ctx, RowKind rowKind, Message message)
      throws MessagingException {
    var row = new GenericRowData(fieldNames.length);
    row.setRowKind(rowKind);

    for (int i = 0; i < fieldNames.length; i++) {
      switch (fieldNames[i].toUpperCase()) {
        case "SUBJECT":
          row.setField(i, StringData.fromString(message.getSubject()));
          break;
        case "SENT":
          row.setField(i, TimestampData.fromInstant(message.getSentDate().toInstant()));
          break;
        case "RECEIVED":
          row.setField(i, TimestampData.fromInstant(message.getReceivedDate().toInstant()));
          break;
        case "TO":
          row.setField(i, mapAddressItems(message.getRecipients(Message.RecipientType.TO)));
          break;
        case "CC":
          row.setField(i, mapAddressItems(message.getRecipients(Message.RecipientType.CC)));
          break;
        case "BCC":
          row.setField(i, mapAddressItems(message.getRecipients(Message.RecipientType.BCC)));
          break;
        case "RECIPIENTS":
          row.setField(i, mapAddressItems(message.getAllRecipients()));
          break;
        case "REPLYTO":
          row.setField(i, mapAddressItems(message.getReplyTo()));
          break;
        case "HEADERS":
          row.setField(i, mapHeaders(message.getAllHeaders()));
          break;
        case "FROM":
          row.setField(i, mapAddressItems(message.getFrom()));
          break;
        case "MESSAGE":
          row.setField(i, StringData.fromString(getMessageContent(message)));
          break;
      }
    }

    ctx.collect(row);
  }

  private Properties getImapProperties() {
    Properties props = new Properties();
    props.put("mail.store.protocol", "imap");
    props.put("mail.imap.ssl.enable", connectorOptions.isSsl());
    props.put("mail.imap.auth", "true");
    props.put("mail.imap.host", connectorOptions.getHost());
    if (connectorOptions.getPort() != null) {
      props.put("mail.imap.port", connectorOptions.getPort());
    }

    return props;
  }

  private void enterWaitLoop() throws Exception {
    long nextReadTimeMs = System.currentTimeMillis();

    while (running) {
      if (connectorOptions.isIdle() && supportsIdle) {
        try {
          ((IMAPFolder) folder).idle(true);
        } catch (MessagingException ignored) {
          supportsIdle = false;
        } catch (IllegalStateException ignored) {
        }
      } else {
        try {
          // Trigger some IMAP request to force the server to send a notification
          folder.getMessageCount();
        } catch (MessagingException ignored) {
        }

        nextReadTimeMs += connectorOptions.getInterval().toMillis();
        Thread.sleep(Math.max(0, nextReadTimeMs - System.currentTimeMillis()));
      }
    }
  }
}

package com.github.airblader;

import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.*;
import jakarta.mail.event.MessageCountAdapter;
import jakarta.mail.event.MessageCountEvent;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.*;

// TODO Catch exceptions (connect, connection closed)
// TODO Environment variables support
// TODO Optionally also support remove message
// TODO Checkpointing?
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
    store.connect(connectorOptions.getUser(), connectorOptions.getPassword());

    folder = store.getFolder(connectorOptions.getFolder());
    folder.open(Folder.READ_ONLY);

    folder.addMessageCountListener(
        new MessageCountAdapter() {
          @Override
          public void messagesAdded(MessageCountEvent event) {
            collectMessages(ctx, event.getMessages());
          }

          // TODO Support remove messages
        });

    if (connectorOptions.getMode() == ScanMode.ALL) {
      collectMessages(ctx, folder.getMessages());
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

  private void collectMessages(SourceContext<RowData> ctx, Message[] messages) {
    for (Message message : messages) {
      try {
        collectMessage(ctx, message);
      } catch (Exception ignored) {
      }
    }
  }

  private void collectMessage(SourceContext<RowData> ctx, Message message)
      throws MessagingException {
    var row = new GenericRowData(fieldNames.length);
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

  private ArrayData mapHeaders(Enumeration<Header> headers) {
    var headerRows = Collections.list(headers).stream().map(this::mapHeader).toArray();
    return new GenericArrayData(headerRows);
  }

  private RowData mapHeader(Header header) {
    return GenericRowData.of(
        StringData.fromString(header.getName()), StringData.fromString(header.getValue()));
  }

  private ArrayData mapAddressItems(Address[] items) {
    if (items == null) {
      return null;
    }

    var mappedItems =
        Arrays.stream(items).map(Address::toString).map(StringData::fromString).toArray();
    return new GenericArrayData(mappedItems);
  }

  private String getMessageContent(Message message) {
    try {
      var content = message.getContent();
      if (content == null) {
        return null;
      }

      if (content instanceof String) {
        return (String) content;
      }
    } catch (IOException | MessagingException ignored) {
    }

    return null;
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
        // Trigger some IMAP request to force the server to send a notification
        folder.getMessageCount();

        nextReadTimeMs += connectorOptions.getInterval().toMillis();
        Thread.sleep(Math.max(0, nextReadTimeMs - System.currentTimeMillis()));
      }
    }
  }
}

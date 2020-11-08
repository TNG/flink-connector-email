package com.github.airblader.imap;

import static com.github.airblader.imap.MessageUtils.*;

import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.*;
import jakarta.mail.event.MessageCountAdapter;
import jakarta.mail.event.MessageCountEvent;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

// TODO Set connection timeout
// TODO idle(false)?
// TODO Keepalive noop
// TODO Option to mark emails seen
// TODO Exactly once semantics
// TODO scan mode with a defined date to start at
@RequiredArgsConstructor
public class ImapSourceFunction extends RichSourceFunction<RowData> {
  private final ConnectorOptions connectorOptions;
  private final String[] fieldNames;

  private transient boolean running = false;
  private transient Store store;
  private transient Folder folder;

  private volatile boolean supportsIdle = true;
  private FetchProfile fetchProfile;

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    fetchProfile = getFetchProfile();

    var session = Session.getInstance(getImapProperties(), null);
    store = session.getStore();
    try {
      store.connect(connectorOptions.getUser(), connectorOptions.getPassword());
    } catch (MessagingException e) {
      throw new ImapSourceException("Failed to connect to the IMAP server.", e);
    }

    try {
      folder = store.getFolder(connectorOptions.getFolder());
    } catch (MessagingException e) {
      throw new ImapSourceException("Could not get folder " + folder.getName(), e);
    }

    openFolder();
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
  }

  @Override
  public void close() {
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
    try {
      folder.fetch(messages, fetchProfile);
    } catch (MessagingException ignored) {
    }

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
    // TODO STARTTLS?
    props.put("mail.imap.auth", "true");
    props.put("mail.imap.host", connectorOptions.getHost());
    if (connectorOptions.getPort() != null) {
      props.put("mail.imap.port", connectorOptions.getPort());
    }

    return props;
  }

  private FetchProfile getFetchProfile() {
    var fetchProfile = new FetchProfile();
    fetchProfile.add(FetchProfile.Item.ENVELOPE);
    return fetchProfile;
  }

  private void enterWaitLoop() throws Exception {
    long nextReadTimeMs = System.currentTimeMillis();

    while (running) {
      if (connectorOptions.isIdle() && supportsIdle) {
        try {
          ((IMAPFolder) folder).idle();
        } catch (MessagingException ignored) {
          supportsIdle = false;
        } catch (IllegalStateException ignored) {
          openFolder();
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

  private void openFolder() {
    try {
      if (!folder.isOpen()) {
        folder.open(Folder.READ_ONLY);
      }
    } catch (MessagingException e) {
      throw new ImapSourceException("Could not open folder " + folder.getName(), e);
    }
  }
}

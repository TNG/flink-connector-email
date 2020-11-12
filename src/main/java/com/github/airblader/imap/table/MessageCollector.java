package com.github.airblader.imap.table;

import jakarta.mail.Message;
import jakarta.mail.event.MessageCountAdapter;
import jakarta.mail.event.MessageCountEvent;
import java.util.function.BiConsumer;
import lombok.RequiredArgsConstructor;
import org.apache.flink.types.RowKind;

@RequiredArgsConstructor
class MessageCollector extends MessageCountAdapter {
  private final BiConsumer<RowKind, Message[]> consumer;
  private final boolean deletions;

  @Override
  public void messagesAdded(MessageCountEvent event) {
    consumer.accept(RowKind.INSERT, event.getMessages());
  }

  @Override
  public void messagesRemoved(MessageCountEvent event) {
    if (!deletions) {
      return;
    }

    consumer.accept(RowKind.DELETE, event.getMessages());
  }
}

package com.github.airblader;

import jakarta.mail.Address;
import jakarta.mail.Header;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import lombok.experimental.UtilityClass;
import lombok.var;
import org.apache.flink.table.data.*;

@UtilityClass
class MessageUtils {
  public static ArrayData mapHeaders(Enumeration<Header> headers) {
    var headerRows = Collections.list(headers).stream().map(MessageUtils::mapHeader).toArray();
    return new GenericArrayData(headerRows);
  }

  public static RowData mapHeader(Header header) {
    return GenericRowData.of(
        StringData.fromString(header.getName()), StringData.fromString(header.getValue()));
  }

  public static ArrayData mapAddressItems(Address[] items) {
    if (items == null) {
      return null;
    }

    var mappedItems =
        Arrays.stream(items).map(Address::toString).map(StringData::fromString).toArray();
    return new GenericArrayData(mappedItems);
  }

  public static String getMessageContent(Message message) {
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
}

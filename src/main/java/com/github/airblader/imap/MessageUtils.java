package com.github.airblader.imap;

import jakarta.mail.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import lombok.experimental.UtilityClass;
import lombok.var;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

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

  public static Object mapAddressItems(Address[] items, LogicalTypeRoot typeRoot) {
    if (items == null) {
      return null;
    }

    return typeRoot.equals(LogicalTypeRoot.ARRAY) && items.length >= 1
        ? mapAddressItems(items)
        : mapAddressItem(items[0]);
  }

  public static StringData mapAddressItem(Address item) {
    if (item == null) {
      return null;
    }

    return StringData.fromString(item.toString());
  }

  public static ArrayData mapAddressItems(Address[] items) {
    if (items == null) {
      return null;
    }

    var mappedItems = Arrays.stream(items).map(MessageUtils::mapAddressItem).toArray();
    return new GenericArrayData(mappedItems);
  }

  public static String getMessageContent(Part part) {
    try {
      var content = part.getContent();
      if (content == null) {
        return null;
      }

      if (content instanceof String) {
        return (String) content;
      }

      if (part.isMimeType("multipart/*")) {
        var multiPart = (Multipart) content;
        for (int i = 0; i < multiPart.getCount(); i++) {
          var partContent = getMessageContent(multiPart.getBodyPart(i));
          if (partContent != null) {
            return partContent;
          }
        }
      }
    } catch (IOException | MessagingException ignored) {
    }

    return null;
  }
}

package com.tngtech.flink.connector.email.common;

import com.tngtech.flink.connector.email.smtp.SmtpSinkException;
import jakarta.mail.Address;
import jakarta.mail.Header;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import lombok.experimental.UtilityClass;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.*;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.stream.IntStream;

@UtilityClass
@Internal
public class MessageUtil {

    public static @Nullable ArrayData encodeAddresses(@Nullable Address[] items) {
        if (items == null) {
            return null;
        }

        final StringData[] mappedItems = Arrays.stream(items)
            .map(MessageUtil::encodeAddress)
            .map(StringData::fromString)
            .toArray(StringData[]::new);

        return new GenericArrayData(mappedItems);
    }

    public static @Nullable String encodeFirstAddress(@Nullable Address[] items) {
        if (items == null) {
            return null;
        }

        return items.length > 0 ? encodeAddress(items[0]) : null;
    }

    public static @Nullable String encodeAddress(@Nullable Address address) {
        return address != null ? address.toString() : null;
    }

    public static @Nullable Address[] decodeAddresses(@Nullable ArrayData addressesData) {
        if (addressesData == null) {
            return null;
        }

        return IntStream.range(0, addressesData.size())
            .mapToObj(addressesData::getString)
            .map(StringData::toString)
            .map(MessageUtil::decodeAddress)
            .toArray(Address[]::new);
    }

    public static @Nullable Address decodeAddress(@Nullable String address) {
        if (address == null) {
            return null;
        }

        try {
            return new InternetAddress(address, false);
        } catch (AddressException e) {
            throw SmtpSinkException.propagate(e);
        }
    }

    // ---------------------------------------------------------------------------------------------

    public static ArrayData encodeHeaders(Enumeration<Header> headers) {
        final RowData[] headerRows = Collections.list(headers).stream()
            .map(MessageUtil::encodeHeader)
            .toArray(RowData[]::new);

        return new GenericArrayData(headerRows);
    }

    public static RowData encodeHeader(Header header) {
        return GenericRowData.of(
            StringData.fromString(header.getName()),
            StringData.fromString(header.getValue())
        );
    }

}

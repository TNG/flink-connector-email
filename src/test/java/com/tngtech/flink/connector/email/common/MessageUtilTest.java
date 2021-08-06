package com.tngtech.flink.connector.email.common;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import org.apache.flink.table.data.ArrayData;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.tngtech.flink.connector.email.common.MessageUtil.encodeAddresses;
import static com.tngtech.flink.connector.email.common.MessageUtil.encodeFirstAddress;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RunWith(DataProviderRunner.class)
public class MessageUtilTest {

    @Test
    @DataProvider(value = {
        "a@one.ext | a@one.ext",
        "a@one.ext | a@one.ext | b@two.ext",
        "null"
    }, splitBy = "\\|")
    public void testConvertFirstAddress(String expected, InternetAddress... addresses) {
        assertThat(encodeFirstAddress(addresses)).isEqualTo(expected);
    }

    @Test
    @DataProvider(value = {
        "a@one.ext",
        "a@one.ext | b@two.ext",
        "Jon Doe <jon.doe@acme.org> | Jane Doe <jane.doe@acme.org>"
    }, splitBy = "\\|")
    public void testConvertAddresses(String... addresses) {
        final InternetAddress[] internetAddresses = Arrays.stream(addresses)
            .map(MessageUtilTest::toAddress)
            .toArray(InternetAddress[]::new);

        final ArrayData convertedAddresses = encodeAddresses(internetAddresses);
        assertThat(convertedAddresses)
            .isNotNull()
            .extracting(ArrayData::size)
            .isEqualTo(addresses.length);
        for (int i = 0; i < addresses.length; i++) {
            assertThat(convertedAddresses.getString(i).toString()).isEqualTo(addresses[i]);
        }
    }

    private static InternetAddress toAddress(String address) {
        try {
            return new InternetAddress(address);
        } catch (AddressException e) {
            return fail("'%s' is not a valid address.", address);
        }
    }

}

package com.tngtech.flink.connector.email.smtp;

import com.tngtech.flink.connector.email.common.ConnectorOptions;
import com.tngtech.flink.connector.email.common.Protocol;
import com.tngtech.flink.connector.email.imap.ImapConfigOptions;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

import static com.tngtech.flink.connector.email.imap.ImapConfigOptions.SSL;
import static com.tngtech.flink.connector.email.smtp.SmtpConfigOptions.*;

@PublicEvolving
@Data
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class SmtpSinkOptions extends ConnectorOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---------------------------------------------------------------------------------------------

    public static SmtpSinkOptions fromOptions(ReadableConfig options) {
        return SmtpSinkOptions.builder()
            .host(options.get(ImapConfigOptions.HOST))
            .port(options.get(PORT))
            .user(options.get(USER))
            .password(options.get(PASSWORD))
            .protocol(options.get(SSL) ? Protocol.SMTPS : Protocol.SMTP)
            .build();
    }
}

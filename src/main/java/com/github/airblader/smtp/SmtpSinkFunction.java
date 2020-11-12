package com.github.airblader.smtp;

import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.var;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

@RequiredArgsConstructor
public class SmtpSinkFunction extends RichSinkFunction<RowData> {
  private final SmtpSinkOptions connectorOptions;
  private final String[] fieldNames;

  private transient Session session;

  @Override
  public void open(Configuration parameters) throws Exception {
    session = Session.getInstance(getSmtpProperties(), null);
  }

  @Override
  public void invoke(RowData value, Context context) throws Exception {
    var message = new MimeMessage(session);
    message.setFrom("jon@acme.org");
    message.setRecipients(Message.RecipientType.TO, "jon@acme.org");
    message.setSubject("Send Test");
    message.setText("42");
    message.setSentDate(new Date());

    message.setHeader("X-Mailer", "SMTP Table Sink");

    Transport.send(
        message, connectorOptions.getEffectiveUser(), connectorOptions.getEffectivePassword());
  }

  // FIXME Unify with ImapUtils#getImapProperties
  private Properties getSmtpProperties() {
    Properties props = new Properties();
    props.put("mail.store.protocol", "smtp");
    props.put("mail.smtp.ssl.enable", connectorOptions.isSsl());
    props.put("mail.smtp.auth", "true");
    props.put("mail.imap.starttls.enable", true);
    props.put("mail.smtp.host", connectorOptions.getEffectiveHost());
    if (connectorOptions.getEffectivePort() != null) {
      props.put("mail.smtp.port", connectorOptions.getEffectivePort());
    }

    return props;
  }
}

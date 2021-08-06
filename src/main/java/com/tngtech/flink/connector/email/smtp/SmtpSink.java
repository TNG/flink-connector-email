package com.tngtech.flink.connector.email.smtp;

import com.tngtech.flink.connector.email.common.SessionProperties;
import com.tngtech.flink.connector.email.common.SubRowData;
import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.util.ByteArrayDataSource;
import lombok.RequiredArgsConstructor;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.UserCodeClassLoader;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

@PublicEvolving
@RequiredArgsConstructor
public class SmtpSink extends RichSinkFunction<RowData> {

    private final DataType physicalRowType;
    private final @Nullable SerializationSchema<RowData> contentSerializer;
    private final SmtpSinkOptions options;
    private final List<WritableMetadata> metadataKeys;

    private transient Session session;
    private transient Transport transport;

    @Override
    public void open(Configuration parameters) throws Exception {
        connect();

        if (contentSerializer != null) {
            contentSerializer.open(new SerializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return getRuntimeContext().getMetricGroup();
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return (UserCodeClassLoader) Thread.currentThread().getContextClassLoader();
                }
            });
        }
    }

    @Override
    public void close() throws Exception {
        if (transport != null) {
            transport.close();
        }
    }

    @Override
    public void invoke(RowData row, Context context) throws Exception {
        final MimeMessage message = new MimeMessage(session);

        int currentPosition = 0;
        if (contentSerializer != null) {
            final RowData contentRow = new SubRowData(row, 0, physicalRowType.getChildren().size());
            final byte[] content = contentSerializer.serialize(contentRow);
            final DataSource dataSource = new ByteArrayDataSource(content, "text/plain");

            final MimeBodyPart bodyPart = new MimeBodyPart();
            bodyPart.setDataHandler(new DataHandler(dataSource));

            final MimeMultipart multipart = new MimeMultipart();
            multipart.addBodyPart(bodyPart);

            message.setContent(multipart);

            currentPosition++;
        } else {
            message.setText("");
        }

        for (WritableMetadata metadata : metadataKeys) {
            metadata.getConverter().convert(row, currentPosition, message);
            currentPosition++;
        }

        message.saveChanges();
        transport.sendMessage(message, message.getAllRecipients());
    }

    // ---------------------------------------------------------------------------------------------

    private void connect() throws Exception {
        session = Session.getInstance(getSmtpProperties(options));
        transport = session.getTransport(options.getProtocol().getName());
        if (options.usesAuthentication()) {
            transport.connect(options.getUser(), options.getPassword());
        } else {
            transport.connect();
        }
    }

    private static Properties getSmtpProperties(SmtpSinkOptions options) {
        return new SessionProperties(options).getProperties();
    }
}

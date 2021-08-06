package com.tngtech.flink.connector.email.imap;

import com.sun.mail.imap.IMAPFolder;
import jakarta.mail.MessagingException;
import org.apache.flink.annotation.Internal;

import java.time.Duration;

/**
 * The IMAP IDLE protocol doesn't actually idle forever, servers might eventually stop sending
 * notifications if they deem the client timed out. This generally happens after ~30 minutes, though
 * some clients are known to go down to as much as ~10 minutes. We therefore need to send a periodic
 * "hearbeat" in the shape of a noop command.
 */
@Internal
class Heartbeat extends Thread {
    private final IMAPFolder folder;
    private final Duration heartbeatInterval;

    public Heartbeat(IMAPFolder folder, Duration heartbeatInterval) {
        super("IMAP Idle Heartbeat");

        this.folder = folder;
        this.heartbeatInterval = heartbeatInterval;
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(heartbeatInterval.toMillis());

                folder.doCommand(
                    protocol -> {
                        protocol.simpleCommand("NOOP", null);
                        return null;
                    });
            } catch (InterruptedException | MessagingException ignored) {
                // We want this thread to just stop
            }
        }
    }
}

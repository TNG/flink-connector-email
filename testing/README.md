# Local Testing

This `docker-compose` setup starts containers for Greenmail (mail server), MinIO (storage), and a
Flink cluster. It then configures Flink's SQL client to connect against that cluster.

Make sure you have the following tools installed:

* `docker`
* `docker-compose`
* `mailutils`

Running the following script will build the project, copy the JAR to the correct location, build and
run the Docker images and start the SQL client:

```
./build_and_run.sh

# Or this to only start the client without (re-)building
./run.sh
```

> :warning: Make sure to run `docker-compose down` to shut down all containers when you're done.

## Connecting

Use the following configuration options to connect using either the IMAP source connector or SMTP
sink connector:

* `'host' = 'greenmail'`
* `'port' = '3143'` (IMAP)
* `'port' = '3025'` (SMTP)
* `user` can be one of jon, jane, alice, or bob.
* `password` is the same as the username.

Example:

<!-- @formatter:off -->
```sql
CREATE TABLE inbox (
    uid STRING NOT NULL METADATA,
    subject STRING METADATA,
    content STRING
) WITH (
    'connector' = 'imap',
    'host' = 'greenmail',
    'user' = 'jon',
    'password' = 'jon'
);
```
<!-- @formatter:on -->

Two IMAP catalogs called "jon" and "jane" have also already been created for the respective users,
so you can also use the catalog directly:

<!-- @formatter:off -->
```sql
SELECT * FROM jon.folders.INBOX;
```
<!-- @formatter:on -->

## Sending Mails

`send-mail.sh` is a small convenience script which you can use to send messages on the local
Greenmail server.

```
# Send message from jane@ to jon@
./send-mail.sh -f jane -t jon -s "Subject" -m "Message"
```

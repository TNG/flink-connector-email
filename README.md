# Introduction

Connect directly to your email server using Flink. This project provides an IMAP source connector,
an IMAP catalog, and a SMTP sink connector. These allow you to read (and send) emails directly from
Flink.

The connectors are written for Flink's Table API and SQL. They are not meant to be used with the
DataStream API.

> :warning: This project is not considered production-ready.
> It is currently intended to be more of a playground project.

# Getting Started

We currently do not publish this connector as a package. If you want to try it out, clone this
repository and take a look at [testing/README.md](testing/README.md). There you will find a
self-contained docker-compose setup along with a dockerized local mail server.

# Connectors

## IMAP

Exposes a specific folder on an IMAP server as a table source:

<!-- @formatter:off -->
```sql
CREATE TABLE inbox (
    uid STRING NOT NULL METADATA,
    subject STRING METADATA,
    content STRING
) WITH (
    'connector' = 'imap',
    'host' = '…',
    'user' = '…',
    'password' = '…'
);
```
<!-- @formatter:on -->

Most message information are exposed through metadata. The only information exposed through physical
columns is the message content itself, which is deserialized using a given format. By default,
the `raw` format is used, meaning that a single physical column of type `STRING` can be declared to
contain the content.

### Configuration

Property           | Type     | Required | Default     | Description
-------------------|----------|----------|-------------|------------
host               | String   | Yes      |             |
user               | String   | Yes      |             |
password           | String   | Yes      |             |
port               | Integer  |          | (automatic) | Port of the IMAP server. If omitted, the default IMAP port is used.
ssl                | Boolean  | Yes      | false       | Whether to connect using SSL.
folder             | String   | Yes      | INBOX       | Name of the IMAP folder to use.
format             | String   | Yes      | raw         | Format with which to decode the message content.
mode               | Enum     | Yes      | all         | Set to "new" to only collect new messages arriving (unbounded), "all" to also fetch existing messages (unbounded), or "current" to only fetch existing emails and finish (bounded).
offset             | Long     |          |             | If set, existing messages are only read starting from this specified UID. This requires "mode" to be "all" or "current".
batch-size         | Integer  |          | 50          | Defines how many existing messages are queried at a time. This requires "mode" to be "all".
connection.timeout | Duration |          | 1min        | Timeout when connecting to the server before giving up.
heartbeat.interval | Duration |          | 15min       | How often to send a heartbeat request to the IMAP server to keep the IDLE connection alive.
interval           | Duration |          | 1s          | If the IMAP server does not support the IDLE protocol, the connector falls back to polling. This defines the interval with which to do so.

### Metadata

Key         | Type
------------|---------------------------------------------
uid         | `BIGINT NOT NULL`
subject     | `STRING`
sent        | `TIMESTAMP WITH LOCAL TIMEZONE(3) NOT NULL`
received    | `TIMESTAMP WITH LOCAL TIMEZONE(3) NOT NULL`
from        | `ARRAY<STRING>`
fromFirst   | `STRING`
to          | `ARRAY<STRING>`
toFirst     | `STRING`
cc          | `ARRAY<STRING>`
bcc         | `ARRAY<STRING>`
recipients  | `ARRAY<STRING>`
replyTo     | `ARRAY<STRING>`
contentType | `STRING`
sizeInBytes | `INT NOT NULL`
seen        | `BOOLEAN`
draft       | `BOOLEAN`
answered    | `BOOLEAN`
headers     | `ARRAY<ROW<key STRING, value STRING>>`

## SMTP

Exposes an SMTP server as a table sink, effectively allowing to send emails by writing into the
sink.

> :warning: This is an early-stage prototype and currently only supports sending plain text emails.

<!-- @formatter:off -->
```sql
CREATE TABLE outbox (
    subject STRING NOT NULL METADATA,
    `from` ARRAY<STRING> METADATA,
    `to` ARRAY<STRING> METADATA,
    content STRING
) WITH (
    'connector' = 'smtp',
    'host' = '…',
    'user' = '…',
    'password' = '…'
);
```
<!-- @formatter:on --> 

Most message information can be written through metadata. The only information writable through
physical columns is the message content itself, which is serialized using a given format. By
default, the `raw` format is used, meaning that a single physical column of type `STRING` can be
declared to write the content to.

### Configuration

Property           | Type     | Required | Default     | Description
-------------------|----------|----------|-------------|------------
host               | String   | Yes      |             |
user               | String   |          |             |
password           | String   |          |             |
port               | Integer  |          | (automatic) | Port of the SMTP server. If omitted, the default SMTP port is used.
ssl                | Boolean  | Yes      | false       | Whether to connect using SSL.
format             | String   | Yes      | raw         | Format with which to encode the message content.

### Metadata

Key         | Type
------------|---------------------------------------------
subject     | `STRING`
from        | `ARRAY<STRING>`
to          | `ARRAY<STRING>`
cc          | `ARRAY<STRING>`
bcc         | `ARRAY<STRING>`
replyTo     | `ARRAY<STRING>`

# Catalogs

## IMAP

Lists all folders on the IMAP server and exposes them as table sources using the `imap` source
connector above.

<!-- @formatter:off -->
```sql
CREATE CATALOG mail WITH (
    'type' = 'imap',
    'host' = '…',
    'user' = '…',
    'password' = '…'
);
```
<!-- @formatter:on --> 

The default (and only) database in an IMAP catalog is called `folders`.

Write operations on the catalog are generally not supported as there is no backing persistence
layer, but rather the catalog acts directly on the IMAP server. The catalog is useful for quick
discovery, and the tables can then be stored in a persistent catalog instead.

You can use dynamic table hints to pass any custom options to the source tables in the catalog:

<!-- @formatter:off -->
```sql
SELECT * FROM mail.folders.INBOX /*+ OPTIONS ('mode' = 'new') */;
```
<!-- @formatter:on -->

# License

See `LICENSE`.

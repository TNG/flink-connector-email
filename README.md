# flink-connector-imap

Implements a (source) connector and a custom catalog to connect to IMAP servers in Apache Flink®.

> :warning: This project is not considered production-ready.

## Getting Started

If you just want to get started quickly and play around with this project, have a look at
[testing/README.md](https://github.com/Airblader/flink-connector-imap/blob/master/testing/README.md)
for a self-contained Docker setup.

Another straight-forward option is to use 
[Ververica Platform Community Edition](https://www.ververica.com/getting-started). With Ververica
Platform you can easily deploy applications to a (local or remote) Kubernetes cluster and also
make use of Apache Flink® SQL in an integrated web user interface. For a five minute, self-contained
setup take a look at [Ververica Platform Playground](https://github.com/ververica/ververica-platform-playground).

## Catalog

`ImapCatalog` is a [Catalog](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/catalogs.html)
implementation. It connects to an IMAP server and automatically discovers all available (sub-)folders
and exposes them as tables in the default database using the IMAP connector.

You can also create the catalog using a DDL statement in Apache Flink® SQL:

```
CREATE CATALOG Mails WITH (
  'type' = 'imap',
  'host' = 'imap.acme.org',
  'user' = 'jon.doe@acme.org',
  'password' = '…',
  'ssl' = 'true'
)
```

All catalog options are forwarded to the IMAP connector in the tables. The catalog supports the
following properties:

Property | Type | Default | Description
---------------------------------------
host | string | (none) | Hostname of the IMAP server
host.env | string | (none) | Environment variable specifying the hostname of the IMAP server (overrides `host`)
port | int | (inferred) | Port of the IMAP server 
port.env | string | (none) | Environment variable specifying the port of the IMAP server (overrides `port`)
user | string | (none) | Username for authentication
user.env | string | (none) | Environment variable specifying the username (overrides `user`)
password | string | (none) | Password for authentication
password.env | string | (none) | Environment variable specifying the password (overrides `password`)
ssl | boolean | `true` | Whether to connect using SSL
scan.startup.timeout | duration | `60s` | Timeout for the socket connection 

You can set further table-specific properties (see below) using 
[hints](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/sql/hints.html):

```
SELECT * FROM Mails.default.Inbox /*+ OPTIONS('scan.startup.mode' = 'all') */;
```

Since the catalog connects directly against the IMAP server and has no persistent storage mechanism
there is currently no way to create tables and persist these options. However, you can choose to
use any other catalog implementation and use the IMAP connector directly instead.

## Connector

The IMAP connector exposes a specific folder on the IMAP server as a table. The following configuration
properties are supported:

Property | Type | Default | Description
---------------------------------------
host | string | (none) | Hostname of the IMAP server
host.env | string | (none) | Environment variable specifying the hostname of the IMAP server (overrides `host`)
port | int | (inferred) | Port of the IMAP server
port.env | string | (none) | Environment variable specifying the port of the IMAP server (overrides `port`)
user | string | (none) | Username for authentication
user.env | string | (none) | Environment variable specifying the username (overrides `user`)
password | string | (none) | Password for authentication
password.env | string | (none) | Environment variable specifying the password (overrides `password`)
ssl | boolean | `true` | Whether to connect using SSL
scan.startup.timeout | duration | `60s` | Timeout for the socket connection
scan.startup.mode | string | `latest` | Use "latest" to fetch only emails which arrive after connecting, "all" to fetch all emails in the folder and "uid" to start at a specific UID
scan.startup.uid | long | (none) | UID to start fetching from upon connecting, only applicable if `scan.startup.mode` is set to "uid"
scan.startup.batch-size | int | `25` | Batch size for fetching existing emails when `scan.startup.mode` is "all" or "uid"
scan.idle | boolean | `true` | Use the IDLE feature instead of polling (automatically falls back to polling)
scan.interval | duration | `1s` | Interval between polling attempts (only if IDLE is not used)
scan.deletions | boolean | `false` | Remove emails from the table if they have been deleted from the server
scan.idle.heartbeat | boolean | `true` | Whether to emit a periodic heartbeat (only if IDLE is used)
scan.idle.heartbeat.interval | duration | `15min` | How frequently to emit the heartbeat (if enabled)
scan.format.address | string | `default` | How to format addresses ("default" for the full address, "simple" for only the email address)
scan.folder | string | `Inbox` | The folder to observe

Note that the IDLE heartbeat interval is chosen with a sane default value, but depending on the IMAP
implementation of the server, a shorter timeout might be required.

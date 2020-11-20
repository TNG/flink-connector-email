# Local Testing

## Prerequisites

1. Install mailutils (we need mailx)
2. Run greenmail through `docker-compose up -d`

## Setup

You can send an email using

```
# Authenticate as jon, send to alice
echo "Message" | mailx -Sv15-compat -Smta=smtp://jon:1234@localhost:3025 -s"Subject" jon@acme.org
```

The users "jon", "alice" and "bob" are preconfigured, each with the same password.

The host IP to use in the table properties is essentially localhost, though if you're running this from a local Kubernetes cluster it may differ:

```
CREATE TEMPORARY TABLE Inbox (
  `uid` BIGINT,
  `subject` STRING,
  `sent` TIMESTAMP(6),
  `received` TIMESTAMP(6),
  `to` ARRAY<STRING>,
  `cc` ARRAY<STRING>,
  `bcc` ARRAY<STRING>,
  `recipients` ARRAY<STRING>,
  `replyTo` ARRAY<STRING>,
  `headers` ARRAY<ROW<`k` STRING, `v` STRING>>,
  `from` ARRAY<STRING>
)
COMMENT ''
WITH (
  'connector' = 'imap',
  'host' = 'localhost', -- e.g. 10.0.1.1 for microk8s with 'host-access' enabled
  'port' = '3143',
  'user' = 'jon',
  'password' = '1234',
  'ssl' = 'false'
); SELECT * FROM Inbox;
```

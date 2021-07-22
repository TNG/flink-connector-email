# Local Testing

This setup starts several docker containers:

1. Greenmail as a mail server
2. minio as a storage
3. A Flink cluster

This provides access to the Flink SQL client which is preconfigured for the Greenmail server.

## Prerequisites

1. Make sure you have docker(-compose) installed.
2. Install mailutils (for mailx).

## Usage

`docker-compose up -d --build`

You can now access a web interface for some services:

1. MinIO: http://localhost:9000/
2. Flink UI: http://localhost:8081/

## SQL Client

`./client.sh`

## Sending Mails

You can send an email using

```
# bob -> alice
echo "Message" | mailx -Sv15-compat -Smta=smtp://bob:bob@localhost:3025 -s"Subject" alice@acme.org
```

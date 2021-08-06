#!/usr/bin/env bash

send_from="jane"
send_to="jon"
subject="Example Message"
message="This is an example message"

while getopts f:t:s:m: option
do
    case "${option}" in
        f) send_from="${OPTARG}" ;;
        t) send_to="${OPTARG}" ;;
        s) subject="${OPTARG}" ;;
        m) message="${OPTARG}" ;;
    esac
done

password="${send_from}"
echo "${message}" | mailx \
  -Sv15-compat \
  -Smta="smtp://${send_from}:${password}@localhost:3025" \
  -s"${subject}" \
  "${send_to}@tngtech.test"

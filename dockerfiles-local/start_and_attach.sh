#!/bin/bash

NPROC=$(nproc)

if [ "$NPROC" -ge 2 ]; then
  trap "echo 'Shutting down' && /opt/AQL/bin/env stop" TERM
  /opt/AQL/bin/env start && sleep 10 && tail -f /opt/AQL/log/console.log & wait ${!}
else
  echo "You need at least 2 cpu cores in your Docker (virtual) machine to run this image!"
  echo "You have ${NPROC} processors"
  exit 1
fi

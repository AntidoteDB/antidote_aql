#!/bin/bash
set -e

if [ ! -f /opt/antidote/releases/0.0.1/setup_ok ]; then
  cd /opt/antidote/releases/0.0.1/
  cp vm.args vm.args_backup
  if [ "$SHORT_NAME" = "true" ]; then
    sed "s/-name /-sname /" vm.args_backup > vm.args
  fi
  touch setup_ok
  
  cd /opt/AQL/scripts
  cp start_shell.sh start_shell_backup.sh
  if [ "$SHORT_NAME" = "true" ]; then
  	sed "s/-name /-sname /" start_shell_backup.sh > start_shell.sh
  fi
fi

exec "$@"

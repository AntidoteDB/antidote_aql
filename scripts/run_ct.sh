#!/bin/bash

if [ -z "$AQL_HOME" ]; then
	export AQL_HOME=$PWD
fi

REBAR=$AQL_HOME/rebar3
TEST_LOGS=$1
NODE_NAME=$2
COOKIE=$3

HOME_DESC=$(echo $PWD | sed 's_/_\\/_g')

cp config/sys-ct.config config/sys-ct.bak.config
sed -i "s/{AQL_DIR}/${HOME_DESC}/g" config/sys-ct.config

$REBAR ct --logdir $TEST_LOGS --dir test --include include --name $NODE_NAME --setcookie $COOKIE --sys_config config/sys-ct.config || rm -rf config/sys-ct.config && mv config/sys-ct.bak.config config/sys-ct.config

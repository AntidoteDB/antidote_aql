#!/usr/bin/env bash

if [ -z "$AQL_NAME" ]; then
	export AQL_NAME=aql@127.0.0.1
fi
if [ -z "$COOKIE" ]; then
	export COOKIE=antidote
fi

erl -pa ./_build/default/lib/aql/ebin -name $AQL_NAME -setcookie $COOKIE -noshell -eval "aqlparser:start_shell()"

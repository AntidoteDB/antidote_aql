#!/usr/bin/env bash

if [ -z "$AQL_NAME" ]; then
	export AQL_NAME='aql@127.0.0.1'
fi

if [ -z "$NODE_NAME" ]; then
	export NODE_NAME='antidote@127.0.0.1'
fi

# echo "Using AQL node name: $AQL_NAME"
# echo "Using Antidote node name: $NODE_NAME"

erl -pa ./_build/default/lib/aql/ebin -name $AQL_NAME -setcookie antidote -noshell -eval "aqlparser:start_shell('$NODE_NAME')" -s erlang halt

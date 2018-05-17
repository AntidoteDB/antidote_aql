#!/usr/bin/env bash

if [ -z "$AQL_NAME" ]; then
	export AQL_NAME='aql@127.0.0.1'
fi
if [ -z "$NODE_NAME" ]; then
	export NODE_NAME='antidote@127.0.0.1'
fi

QUERY=$(echo "$1" | sed -r 's/[*]/\\*/g') # $1 = query to be issued

erl -pa ../_build/default/lib/aql/ebin -name $AQL_NAME -setcookie antidote -noshell -eval "Res = aqlparser:parse({str, \"$QUERY\"}, '$NODE_NAME', undefined), io:format(\"~p~n~n\", [Res])" -s erlang halt

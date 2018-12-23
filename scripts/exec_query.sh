#!/usr/bin/env bash

if [ -z "$NODE_NAME" ]; then
	export NODE_NAME='aql@127.0.0.1'
fi

if [ -z "$AQL_REL" ]; then
	# by default, this script will be executed from the Makefile
	export AQL_REL=_build/default/rel/aql
fi

QUERY=$(echo "$1" | sed -r 's/[*]/\\*/g') # $1 = query to be issued

$AQL_REL/bin/env start && sleep 5
$AQL_REL/bin/env eval "Res = aql:query(\"$QUERY\"), io:format(\"~p~n~n\", [Res])"
$AQL_REL/bin/env stop


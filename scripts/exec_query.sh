#!/usr/bin/env bash

if [ -z "$NODE_NAME" ]; then
	export NODE_NAME='aql@127.0.0.1'
fi

QUERY=$(echo "$1" | sed -r 's/[*]/\\*/g') # $1 = query to be issued

../_build/default/rel/aql/bin/env start && sleep 5
../_build/default/rel/aql/bin/env eval "Res = aql:query(\"$QUERY\"), io:format(\"~p~n~n\", [Res])"
../_build/default/rel/aql/bin/env stop

#erl -pa ../_build/default/lib/aql/ebin -name $AQL_NAME -setcookie antidote -noshell -eval "Res = aqlparser:parse({str, \"$QUERY\"}, '$NODE_NAME', undefined), io:format(\"~p~n~n\", [Res])" -s erlang halt

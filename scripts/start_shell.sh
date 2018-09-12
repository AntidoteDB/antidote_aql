#!/usr/bin/env bash

if [ -z "$NODE_NAME" ]; then
	export NODE_NAME='aql@127.0.0.1'
fi

# echo "Using AQL node name: $NODE_NAME"

_build/default/rel/aql/bin/env start && sleep 5 && _build/default/rel/aql/bin/env eval "aql:start_shell()"

#!/bin/bash
if [ -z "$NODE_NAME" ]; then
	export NODE_NAME='aql@127.0.0.1'
fi

_build/default/rel/aql/bin/env console

#!/usr/bin/env bash

_build/default/rel/aql/bin/env start && sleep 5 && _build/default/rel/aql/bin/env eval "aql:start_shell()"

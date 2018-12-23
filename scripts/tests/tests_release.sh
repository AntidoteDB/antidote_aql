#!/bin/bash

CURR_DIR=$PWD
AQL_HOME=~/Desktop/AQL
AQL_REL=$AQL_HOME/_build/default/rel/aql

source $1
TESTNUM=$2
TESTCONTENT=TEST$TESTNUM[@]
TEST=("${!TESTCONTENT}")
	
## Deleting the old database state
function reset_db {
	echo "> Resetting the database..."
	rm -rf $AQL_REL/*
}

function start_aql {
	echo "> Starting the AQL node..."
	killall beam.smp
	cd $AQL_HOME && make release
	#$AQL_REL/bin/env start && tail -f $AQL_REL/log/console.log &
	$AQL_REL/bin/env start && sleep 10 && tail -f $AQL_REL/log/console.log &
	cd $CURR_DIR
	sleep 10
}

## Creating new tables into the database
function create_db {	
	echo "> Creating the database..."
	ts=$(date +%s%N) #; my_command ; tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Time elapsed: $tt milliseconds"
	array=("${!1}")
	for k in $(seq 0 $((${#array[@]}-1))); do
		#aql_func="Res = aqlparser:parse({str, \"${array[$k]}\"}, '\''$NODE_NAME'\'', $TX), io:format(\"~p~n~n\", [Res])"
		#cmd="$EXEC_CMD '$aql_func' -s erlang halt"

		#eval $cmd
		QUERY=$(echo "${array[$k]}" | sed -r 's/[*]/\\*/g')
		$AQL_REL/bin/env eval "Res = aql:query(\"$QUERY\"), io:format(\"~p~n~n\", [Res])"
	done
	tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Time elapsed: $tt milliseconds"
	echo ""
}

## Initializing the database state by creating new records
function init_db {
	echo "> Initializing the database..."
	ts=$(date +%s%N)
	array=("${!1}")
	for k in $(seq 0 $((${#array[@]}-1))); do
		#aql_func="Res = aqlparser:parse({str, \"${array[$k]}\"}, '\''$NODE_NAME'\'', $TX), io:format(\"~p~n~n\", [Res])"
		#cmd="$EXEC_CMD '$aql_func' -s erlang halt"
		#echo $cmd
		#eval $cmd
		QUERY=$(echo "${array[$k]}" | sed -r 's/[*]/\\*/g')
		$AQL_REL/bin/env eval "Res = aql:query(\"$QUERY\"), io:format(\"~p~n~n\", [Res])"
	done
	tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Time elapsed: $tt milliseconds"
	echo ""
}

## Testing the implementation by issuing high-level queries to the database
function build_query_command {
	query="$1"
	expected="$2"
	#echo "Query: $query"
	#echo "Expected: $expected"
	local cmd="{ok, [Res], _} = aqlparser:parse({str, \"$query\"}, '\''$NODE_NAME'\'', $TX), "
	cmd="$cmd case length(Res) of $expected -> io:format(\"{ok, ~p}~n~n\", [Res]);"
	cmd="$cmd Else -> io:format(\"{error, {expected, $expected}, {got, ~p}}~n~n\", [Else]) end"
	echo $cmd
}

function querying_db {
	echo "> Querying the database:"
	arg=${!1}
	arg2=${!2}
	IFS=';' read -ra queries <<< "$arg"
	read -ra expected <<< "$arg2"
	
	#array=("${!1}")
	for k in $(seq 0 $((${#queries[@]}-1))); do
		inc=$((k + 1))
		echo "$inc. ${queries[$k]}"
		echo "# of Expected Rows: ${expected[$k]}"
		echo "Result:"
		#aql_func=$(build_query_command "${queries[$k]}" "${expected[$k]}")
		#RESULT=$($QUERY_SCRIPT "${queries[$k]}")
		ts=$(date +%s%N)
		QUERY=$(echo "${queries[$k]}" | sed -r 's/[*]/\\*/g')
		$AQL_REL/bin/env eval "Res = aql:query(\"$QUERY\"), io:format(\"~p~n~n\", [Res])"
		tt=$((($(date +%s%N) - $ts)/1000000)) ; echo "Time elapsed: $tt milliseconds"
		#LEN=$((${#RESULT} - 2))
		#IFS=',' read -ra result_split <<< "${RESULT:1:LEN}"
		#if [ "${result_split[0]}" = "ok"]; then
		#	PRINT=$(echo ${RESULT##*,})	
		#	echo "${PRINT:1:$((${#PRINT} - 2))}"
		#else
		#	echo "error: $RESULT"
		#fi
		
		#cmd="$EXEC_CMD '$aql_func' -s erlang halt"
		#echo $cmd
		#eval $cmd
		echo ""
	done
	unset IFS
}

function stop_db {
	echo "> Stopping the AQL node..."
	$AQL_REL/bin/env stop
}

reset_db && start_aql && create_db TEST[0] && init_db TEST[1] && querying_db TEST[2] TEST[3] && stop_db && echo "> Done."

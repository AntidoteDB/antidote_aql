#!/bin/bash

CURR_DIR=$PWD
ANTIDOTE_DIR=~/Desktop/antidote
ANTIDOTE_REL=$ANTIDOTE_DIR/_build/default/rel/antidote
AQL_DIR=~/Desktop/AQL
export AQL_NAME='aql@127.0.0.1'
QUERY_SCRIPT=$AQL_DIR/scripts/exec_query.sh

export NODE_NAME="antidote@127.0.0.1"
TX="undefined"

source $1
TESTNUM=$2
TESTCONTENT=TEST$TESTNUM[@]
TEST=("${!TESTCONTENT}")
	
## Deleting the old database state
function reset_db {
	echo "> Resetting the database..."
	# rm -rf $ANTIDOTE_DIR/data.antidote@* && rm -rf $ANTIDOTE_DIR/log.antidote@*
	rm -rf $ANTIDOTE_REL/data.antidote@* && rm -rf $ANTIDOTE_REL/log.antidote@* && rm -rf $ANTIDOTE_REL/data
}

function start_antidote {
	echo "> Starting an Antidote node..."
	killall beam.smp
	cd $ANTIDOTE_DIR && make rel
	$ANTIDOTE_REL/bin/env start && tail -f $ANTIDOTE_REL/log/console.log &
	cd $CURR_DIR
	sleep 10s
}

## Creating new tables into the database
function create_db {	
	echo "> Creating the database..."
	array=("${!1}")
	for k in $(seq 0 $((${#array[@]}-1))); do
		#aql_func="Res = aqlparser:parse({str, \"${array[$k]}\"}, '\''$NODE_NAME'\'', $TX), io:format(\"~p~n~n\", [Res])"
		#cmd="$EXEC_CMD '$aql_func' -s erlang halt"

		#eval $cmd
		$QUERY_SCRIPT "${array[$k]}"
	done
}

## Initializing the database state by creating new records
function init_db {
	echo "> Initializing the database..."
	array=("${!1}")
	for k in $(seq 0 $((${#array[@]}-1))); do
		#aql_func="Res = aqlparser:parse({str, \"${array[$k]}\"}, '\''$NODE_NAME'\'', $TX), io:format(\"~p~n~n\", [Res])"
		#cmd="$EXEC_CMD '$aql_func' -s erlang halt"
		#echo $cmd
		#eval $cmd
		$QUERY_SCRIPT "${array[$k]}"
	done
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
		#echo "Expected: ${expected[$k]}"
		echo "Result:"
		#aql_func=$(build_query_command "${queries[$k]}" "${expected[$k]}")
		#RESULT=$($QUERY_SCRIPT "${queries[$k]}")
		$QUERY_SCRIPT "${queries[$k]}"
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
	done
	unset IFS
}

chmod a+x $QUERY_SCRIPT
reset_db && start_antidote && create_db TEST[0] && init_db TEST[1] && querying_db TEST[2] TEST[3] && echo "> Done."

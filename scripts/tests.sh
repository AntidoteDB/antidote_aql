#!/bin/bash

CURR_DIR=$PWD
ANTIDOTE_DIR=~/Desktop/antidote
ANTIDOTE_REL=$ANTIDOTE_DIR/_build/default/rel/antidote
AQL_DIR=~/Desktop/AQL
AQL_NAME='aql@127.0.0.1'

NODE="antidote@127.0.0.1"
TX="undefined"

source $1
TESTNUM=$2
TESTCONTENT=TEST$TESTNUM[@]
TEST=("${!TESTCONTENT}")

EXEC_CMD="erl -pa $AQL_DIR/_build/default/lib/aql/ebin -name $AQL_NAME -setcookie antidote -noshell -eval "

CREATE_DB=(
	"CREATE @AW TABLE Artist(Name VARCHAR PRIMARY KEY, Age INT, Country VARCHAR DEFAULT '\''ENG'\'')"
	"CREATE @RW TABLE Album(Title VARCHAR PRIMARY KEY, Art VARCHAR FOREIGN KEY @FR REFERENCES Artist(Name), Year INT)"
	"CREATE @AW TABLE Track(Title VARCHAR PRIMARY KEY, Alb VARCHAR FOREIGN KEY @IR REFERENCES Album(Title))"
)

INIT_DB=(
	"INSERT INTO Artist (Name, Age) VALUES ('\''Sam'\'', 22)"
	"INSERT INTO Artist (Name, Age, Country) VALUES ('\''Rob'\'', 25, '\''ITA'\'')"
	"INSERT INTO Artist (Name, Age, Country) VALUES ('\''Jon'\'', 40, '\''USA'\'')"
	"INSERT INTO Artist (Name, Age, Country) VALUES ('\''Ken'\'', 33, '\''AUS'\'')"
	
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A0'\'', '\''Sam'\'', 2016)"
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A1'\'', '\''Sam'\'', 2008)"
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A2'\'', '\''Rob'\'', 2012)"
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A3'\'', '\''Jon'\'', 1998)"
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A4'\'', '\''Jon'\'', 2005)"
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A5'\'', '\''Jon'\'', 2001)"
	"INSERT INTO Album (Title, Art, Year) VALUES ('\''A6'\'', '\''Ken'\'', 2006)"
	
	"INSERT INTO Track (Title, Alb) VALUES ('\''Hello'\'', '\''A0'\'')"
	"INSERT INTO Track (Title, Alb) VALUES ('\''World'\'', '\''A0'\'')"
	"INSERT INTO Track (Title, Alb) VALUES ('\''Goodbye'\'', '\''A1'\'')"
)

QUERIES=(
	"SELECT \* FROM Album WHERE Year > 2000 AND Art = '\''Jon'\'' OR Year < 2018 AND Year > 2002;"
	"SELECT \* FROM Album WHERE Year > 2010 OR Art = '\''Jon'\'' AND Year < 2018 AND Art = '\''Ken'\'';"
	"SELECT \* FROM Album WHERE Year > 2010 OR (Art = '\''Jon'\'' AND Year < 2018 AND Art = '\''Ken'\'');"
	"SELECT \* FROM Album WHERE ((Year > 2000 AND Art = '\''Jon'\'') OR Year < 2018) AND Year > 2002;"
)
	
## Deleting the old database state
function reset_db {
	echo "> Resetting the database..."
	rm -rf $ANTIDOTE_DIR/data.antidote@* && rm -rf $ANTIDOTE_DIR/log.antidote@*
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
		aql_func="Res = aqlparser:parse({str, \"${array[$k]}\"}, '\''$NODE'\'', $TX), io:format(\"~p~n~n\", [Res])"
		cmd="$EXEC_CMD '$aql_func' -s erlang halt"
		#echo $cmd
		eval $cmd
		##erl -pa $AQL_DIR/_build/default/lib/aql/ebin -name $AQL_NAME -setcookie antidote -noshell -eval \''Res = aqlparser:parse({str, '${CREATE_DB[$k]}'}, '\'$NODE\'', '\'$TX\''), io:format("~p~n", [Res])'\'
	done
}

## Initializing the database state by creating new records
function init_db {
	echo "> Initializing the database..."
	array=("${!1}")
	for k in $(seq 0 $((${#array[@]}-1))); do
		aql_func="Res = aqlparser:parse({str, \"${array[$k]}\"}, '\''$NODE'\'', $TX), io:format(\"~p~n~n\", [Res])"
		cmd="$EXEC_CMD '$aql_func' -s erlang halt"
		#echo $cmd
		eval $cmd
	done
}

## Testing the implementation by issuing high-level queries to the database
function build_query_command {
	query="$1"
	expected="$2"
	#echo "Query: $query"
	#echo "Expected: $expected"
	local cmd="{ok, [Res], _} = aqlparser:parse({str, \"$query\"}, '\''$NODE'\'', $TX), "
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
		aql_func=$(build_query_command "${queries[$k]}" "${expected[$k]}")
		#aql_func="Res = aqlparser:parse({str, \"${queries[$k]}\"}, '\''$NODE'\'', $TX), io:format(\"~p~n~n\", [Res])"
		cmd="$EXEC_CMD '$aql_func' -s erlang halt"
		#echo $cmd
		eval $cmd
	done
	unset IFS
}

reset_db && start_antidote && create_db TEST[0] && init_db TEST[1] && querying_db TEST[2] TEST[3] && echo "> Done."

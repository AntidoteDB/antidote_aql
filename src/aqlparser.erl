%%%-------------------------------------------------------------------
%% @author Joao
%% @author Pedro Lopes
%% @doc aqlparser public API
%% @end
%%%-------------------------------------------------------------------

-module(aqlparser).

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

-define(DEFAULT_NODE, 'antidote@127.0.0.1').

%% Application callbacks
-export([parse/2, parse/3, start_shell/0, start_shell/1]).

%%====================================================================
%% API
%%====================================================================

-spec parse(input(), node()) -> queryResult() | {err, term()}.
parse(Input, Node) ->
	parse(Input, Node, undefined).

parse({str, "\n"}, _Node, Tx) ->
	{ok, Tx};
parse({str, Query}, Node, Tx) ->
	TokensRes = scanner:string(Query),
	case TokensRes of
		{ok, Tokens, _} ->
			ParseRes = parser:parse(Tokens),
			case ParseRes of
				{ok, ParseTree} ->
					exec(ParseTree, [], Node, Tx);
				_Else ->
					ParseRes
			end;
		_Else ->
			TokensRes
	end;
parse({file, Filename}, Node, Tx) ->
	{ok, File} = file:read_file(Filename),
	Content = unicode:characters_to_list(File),
	parse({str, Content}, Node, Tx).

start_shell() ->
	start_shell(?DEFAULT_NODE).

start_shell(Node) when is_atom(Node) ->
	io:fwrite("Welcome to the AQL Shell.~n"),
	io:format("(connected to node ~p)~n", [Node]),
	read_and_exec(Node, undefined).

read_and_exec(Node, Tx) ->
	Line = io:get_line("AQL> "),
	case parse({str, Line}, Node, Tx) of
		{ok, Res, quit} ->
			io:fwrite("~p~n", [Res]);
		{ok, Res, RetTx} ->
			io:fwrite("~p~n", [Res]),
			read_and_exec(Node, RetTx);
        {error, Msg} ->
            io:fwrite("~p~n", [{error, Msg}]),
            read_and_exec(Node, undefined);
		{ok, RetTx} ->
			read_and_exec(Node, RetTx);
		Else ->
			io:fwrite("~p~n", [Else]),
			read_and_exec(Node, undefined)
	end.

%%====================================================================
%% Internal functions
%%====================================================================

exec([Query | Tail], Acc, Node, Tx) ->
	Res = exec(Query, Node, Tx),
	case Res of
		ok ->
			exec(Tail, Acc, Node, Tx);
		{ok, quit} ->
			case Tx of
				undefined -> ok;
				_ -> abort_transaction(undefined, Tx)
			end,
			{ok, lists:append(Acc, [Res]), quit};
		{ok, {begin_tx, Tx2}} ->
			exec(Tail, lists:append(Acc, [Res]), Node, Tx2);
		{ok, {commit_tx, Tx2}} ->
			CommitRes = commit_transaction({ok, commit_tx}, Tx2),
			exec(Tail, lists:append(Acc, [CommitRes]), Node, undefined);
		{ok, {abort_tx, Tx2}} ->
			AbortRes = abort_transaction({ok, abort_tx}, Tx2),
			exec(Tail, lists:append(Acc, [AbortRes]), Node, undefined);
		{ok, NewNode} ->
			exec(Tail, Acc, NewNode, Tx);
		{error, Msg, AbortedTx} ->
			abort_transaction(ignore, AbortedTx),
			exec(Tail, lists:append(Acc, [{error, Msg}]), Node, undefined);
		Res ->
			exec(Tail, lists:append(Acc, [Res]), Node, Tx)
	end;
exec([], Acc, _Node, Tx) ->
	{ok, Acc, Tx}.

commit_transaction(Res, Tx) ->
	CommitRes = antidote:commit_transaction(Tx),
	%ok = antidote:release_locks(lock_mgr_es, Tx),
	case CommitRes of
		{ok, _CT} ->
			Res;
		_Else ->
			{error, CommitRes}
	end.

abort_transaction(Res, Tx) ->
	antidote:abort_transaction(Tx),
	%ok = antidote:release_locks(lock_mgr_es, Tx),
	Res.
	%% TODO to be uncommented when Antidote implements transaction abortion
	%case AbortRes of
	%	{ok, _CT} ->
	%		Res;
	%	_Else ->
	%		{error, AbortRes}
	%end.

exec(?BEGIN_CLAUSE(?TRANSACTION_TOKEN), Node, PassedTx) when is_atom(Node) ->
	case PassedTx of
		undefined ->
			{ok, Tx} = antidote:start_transaction(Node),
			{ok, {begin_tx, Tx}};
		_Else ->
			{error, "There's already an ongoing transaction"}
	end;
exec(?COMMIT_CLAUSE(?TRANSACTION_TOKEN), Node, PassedTx) when is_atom(Node) ->
	case PassedTx of
		undefined ->
			{error, "There's no current ongoing transaction"};
		_Else ->
			{ok, {commit_tx, PassedTx}}
	end;
exec(?ABORT_CLAUSE(?TRANSACTION_TOKEN), Node, PassedTX) when is_atom(Node) ->
	case PassedTX of
		undefined ->
			{error, "There's no current ongoing transaction"};
		_Else ->
			{ok, {abort_tx, PassedTX}}
	end;
exec(?QUIT_CLAUSE(_), _Node, _PassedTx) ->
    {ok, quit};

exec(Query, Node, undefined) when is_atom(Node) ->
	{ok, Tx} = antidote:start_transaction(Node),
	try
		exec(Query, Tx)
	of
		{error, _} = Res ->
			Res;
		Else ->
			commit_transaction(Else, Tx)
	catch
		Reason ->
      abort_transaction(ignore, Tx),
			{error, Reason, Tx}
	end;
exec(Query, Node, PassedTx) when is_atom(Node) ->
  try
		exec(Query, PassedTx)
  of
		Res -> Res
  catch
		Reason ->
			abort_transaction(ignore, PassedTx),
			{error, Reason, PassedTx}
  end.

exec(?SHOW_CLAUSE(?TABLES_TOKEN), Tx) ->
	Tables = table:read_tables(Tx),
	TNames = lists:map(fun({{Name, _Type}, _Value}) -> Name end, Tables),
	io:fwrite("Tables: ~p~n", [TNames]),
	TNames;
exec(?SHOW_CLAUSE({?INDEX_TOKEN, TName}), Tx) ->
	Keys = index:p_keys(TName, Tx),
	lists:foreach(fun({Key, BObj}) ->
		{_Key, _Type, Bucket} = BObj,
		io:fwrite("{key: ~p, table: ~p}~n", [Key, Bucket])
	end, Keys),
	Keys;
exec(?SHOW_CLAUSE({?INDEX_TOKEN, IndexName, TName}), Tx) ->
	FormattedIndex = index:s_keys_formatted(TName, IndexName, Tx),
	lists:foreach(fun({IndexedVal, PKeys}) ->
		PKValOnly = lists:map(fun({Key, _Type, _Bucket}) -> Key end, PKeys),
		io:fwrite("{column value: ~p, primary keys: ~p}~n", [IndexedVal, PKValOnly])
	end, FormattedIndex),
	FormattedIndex;
exec(?SHOW_CLAUSE({?INDEXES_TOKEN, TName}), Tx) ->
	Tables = table:read_tables(Tx),
	Table = table:lookup(TName, Tables),
	Indexes = table:indexes(Table),
	lists:foreach(fun(?T_INDEX(Name, _TName, Cols)) ->
		io:fwrite("{index name: ~p, columns: ~p}~n", [Name, Cols])
	end, Indexes),
	Indexes;

exec(?CREATE_CLAUSE(Table), Tx) when ?is_table(Table) ->
	eval("Create Table", Table, table, Tx);
exec(?CREATE_CLAUSE(Index), Tx) when ?is_index(Index) ->
	eval("Create Index", Index, index, Tx);
exec(?INSERT_CLAUSE(Insert), Tx) ->
	eval("Insert", Insert, insert, Tx);
exec(?DELETE_CLAUSE(Delete), Tx) ->
	eval("Delete", Delete, delete, Tx);
exec({?UPDATE_TOKEN, Update}, Tx) ->
	eval("Update", Update, update, Tx);
exec({?SELECT_TOKEN, Select}, Tx) ->
	eval("Select", Select, select, Tx);
exec(_Invalid, _Node) ->
	throw("Invalid query").

eval(QName, Props, M, Tx) ->
	case M of
		table ->
			Status = M:exec(Props, Tx);
		_Else ->
			Tables = get_table_from_query(M, Props, Tx),
			Status = M:exec(Tables, Props, Tx)
	end,
	eval_status(QName, Status).


eval_status(Query, Status) ->
	%AQuery = list_to_atom(Query),
	case Status of
		ok ->
			%io:fwrite("[Ok] ~p~n", [AQuery]),
			Status;
		{ok, Msg} ->
			%io:fwrite("[Ok] ~p: ~p~n", [AQuery, Msg]),
			Msg;
		error ->
			%io:fwrite("[Err] ~p~n", [AQuery]),
			{error, Query};
		{error, Msg} ->
			%io:fwrite("[Err] ~p: ~p~n", [AQuery, Msg]),
			{error, Msg};
		{badrpc, Msg} ->
			{_Error, Desc} = antidote:handleBadRpc(Msg),
			%io:fwrite("[Err] ~p: ~p~n", [Error, Desc]),
			{error, Desc};
		Msg ->
			%io:fwrite("[????] ~p: ~p~n", [AQuery, Msg]),
			Msg
	end.

get_table_from_query(M, Props, TxId) ->
	TableName = M:table(Props),
	Tables = table:read_tables(TxId),
	Table = table:lookup(TableName, Tables),
	{Table, Tables}.

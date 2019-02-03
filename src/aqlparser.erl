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

%-define(DEFAULT_NODE, 'antidote@127.0.0.1').

%% Application callbacks
-export([parse/1, parse/2, start_shell/0]).

%%====================================================================
%% API
%%====================================================================

-spec parse(input()) -> {ok, term(), term()} | {ok, term()} | {error, term(), term()}.
parse(Input) ->
  parse(Input, undefined).

-spec parse(input(), term()) -> {ok, term(), term()} | {ok, term()} | {error, term(), term()} | {error, term()}.
parse({str, "\n"}, Tx) ->
	{ok, Tx};
parse({str, Query}, Tx) ->
	TokensRes = scanner:string(Query),
	case TokensRes of
		{ok, Tokens, _} ->
			ParseRes = parser:parse(Tokens),
			case ParseRes of
				{ok, ParseTree} ->
					exec(ParseTree, [], Tx);
				_Else ->
					ParseRes
			end;
		_Else ->
			TokensRes
	end;
parse({file, Filename}, Tx) ->
	{ok, File} = file:read_file(Filename),
	Content = unicode:characters_to_list(File),
	parse({str, Content}, Tx).

start_shell() ->
	io:fwrite("Welcome to the AQL Shell.~n"),
	read_and_exec(undefined).

read_and_exec(Tx) ->
	Line = io:get_line("AQL> "),
	case parse({str, Line}, Tx) of
		{ok, Res, quit} ->
			io:fwrite("~p~n", [Res]);
		{ok, Res, RetTx} ->
			io:fwrite("~p~n", [Res]),
			read_and_exec(RetTx);
		{error, Msg, _} ->
			io:fwrite("~p~n", [{error, Msg}]),
			read_and_exec(undefined);
		{error, Msg} ->
			io:fwrite("~p~n", [{error, Msg}]),
			read_and_exec(undefined);
		{ok, RetTx} ->
			read_and_exec(RetTx)
	end.

%%====================================================================
%% Internal functions
%%====================================================================

exec([Query | Tail], Acc, Tx) ->
	Res = exec(Query, Tx),
	case Res of
		ok ->
			exec(Tail, Acc, Tx);
		{ok, quit} ->
			case Tx of
				undefined -> ok;
				_ -> abort_transaction(undefined, Tx)
			end,
			{ok, lists:append(Acc, [Res]), quit};
		{ok, {begin_tx, Tx2}} ->
			exec(Tail, lists:append(Acc, [Res]), Tx2);
		{ok, {commit_tx, Tx2}} ->
			CommitRes = commit_transaction({ok, commit_tx}, Tx2),
			exec(Tail, lists:append(Acc, [CommitRes]), undefined);
		{ok, {abort_tx, Tx2}} ->
			AbortRes = abort_transaction({ok, abort_tx}, Tx2),
			exec(Tail, lists:append(Acc, [AbortRes]), undefined);
		%{ok, NewNode} ->
		%	exec(Tail, Acc, NewNode, Tx);
		{error, Msg, _AbortedTx} ->
			exec(Tail, lists:append(Acc, [{error, Msg}]), undefined);
		Res ->
			exec(Tail, lists:append(Acc, [Res]), Tx)
	end;
exec([], Acc, Tx) ->
	{ok, Acc, Tx}.

commit_transaction(Res, Tx) ->
	CommitRes = antidote_handler:commit_transaction(Tx),
	ok = antidote_handler:release_locks(es_locks, Tx),
	case CommitRes of
		{ok, _CT} ->
			Res;
    {error, Reason} ->
      {error, Reason}
	end.

abort_transaction(Res, Tx) ->
	AbortRes = antidote_handler:abort_transaction(Tx),
	ok = antidote_handler:release_locks(es_locks, Tx),
	case AbortRes of
    ok ->
			Res;
    {error, Reason} ->
      {error, Reason}
	end.

exec(?BEGIN_CLAUSE(?TRANSACTION_TOKEN), PassedTx) ->
	case PassedTx of
		undefined ->
			{ok, Tx} = antidote_handler:start_transaction(),
			{ok, {begin_tx, Tx}};
		_Else ->
			{error, "There's already an ongoing transaction"}
	end;
exec(?COMMIT_CLAUSE(?TRANSACTION_TOKEN), PassedTx) ->
	case PassedTx of
		undefined ->
			{error, "There's no current ongoing transaction"};
		_Else ->
			{ok, {commit_tx, PassedTx}}
	end;
exec(?ROLLBACK_CLAUSE(?TRANSACTION_TOKEN), PassedTX) ->
	case PassedTX of
		undefined ->
			{error, "There's no current ongoing transaction"};
		_Else ->
			{ok, {abort_tx, PassedTX}}
	end;
exec(?QUIT_CLAUSE(_), _PassedTx) ->
    {ok, quit};

exec(Query, undefined) ->
	{ok, Tx} = antidote_handler:start_transaction(),
	try
		execute(Query, Tx)
	of
		{error, _} = Res ->
			Res;
		Else ->
			commit_transaction(Else, Tx)
	catch
		_:Exception ->
      Error = antidote_handler:handleUpdateError(Exception),
      abort_transaction(ignore, Tx),
			{error, Error, Tx}
		%Reason ->
    %  abort_transaction(ignore, Tx),
    %  {error, Reason, Tx}
	end;
exec(Query, PassedTx) ->
  try
		execute(Query, PassedTx)
  of
		Res -> Res
  catch
		_:Exception ->
      Error = antidote_handler:handleUpdateError(Exception),
			abort_transaction(ignore, PassedTx),
			{error, Error, PassedTx}
    %Reason ->
    %  abort_transaction(ignore, PassedTx),
    %  {error, Reason, PassedTx}
  end.

execute(?SHOW_CLAUSE(?TABLES_TOKEN), Tx) ->
	Tables = table:read_tables(Tx),
	TNames = lists:map(fun({{Name, _Type}, _Value}) -> Name end, Tables),
	io:fwrite("Tables: ~p~n", [TNames]),
	TNames;
execute(?SHOW_CLAUSE({?INDEX_TOKEN, TName}), Tx) ->
	Keys = index:p_keys(TName, Tx),
	lists:foreach(fun({Key, BObj}) ->
		{_Key, _Type, Bucket} = BObj,
		io:fwrite("{key: ~p, table: ~p}~n", [Key, Bucket])
	end, Keys),
	Keys;
execute(?SHOW_CLAUSE({?INDEX_TOKEN, IndexName, TName}), Tx) ->
	FormattedIndex = index:s_keys_formatted(TName, IndexName, Tx),
	lists:foreach(fun({IndexedVal, PKeys}) ->
		PKValOnly = lists:map(fun({Key, _Type, _Bucket}) -> Key end, PKeys),
		io:fwrite("{column value: ~p, primary keys: ~p}~n", [IndexedVal, PKValOnly])
	end, FormattedIndex),
	FormattedIndex;
execute(?SHOW_CLAUSE({?INDEXES_TOKEN, TName}), Tx) ->
	Tables = table:read_tables(Tx),
	Table = table:lookup(TName, Tables),
	Indexes = table:indexes(Table),
	lists:foreach(fun(?T_INDEX(Name, _TName, Cols)) ->
		io:fwrite("{index name: ~p, columns: ~p}~n", [Name, Cols])
	end, Indexes),
	Indexes;

execute(?CREATE_CLAUSE(Table), Tx) when ?is_table(Table) ->
	eval("Create Table", Table, table, Tx);
execute(?CREATE_CLAUSE(Index), Tx) when ?is_index(Index) ->
	eval("Create Index", Index, index, Tx);
execute(?INSERT_CLAUSE(Insert), Tx) ->
	eval("Insert", Insert, insert, Tx);
execute(?DELETE_CLAUSE(Delete), Tx) ->
	eval("Delete", Delete, delete, Tx);
execute({?UPDATE_TOKEN, Update}, Tx) ->
	eval("Update", Update, update, Tx);
execute({?SELECT_TOKEN, Select}, Tx) ->
	eval("Select", Select, select, Tx);
execute(_Invalid, _Node) ->
	throw("Invalid query").

eval(QName, Props, table, Tx) ->
  Status = table:exec(Props, Tx),
  eval_status(QName, Status);
eval(QName, Props, M, Tx) ->
  Tables = get_table_from_query(M, Props, Tx),
  Status = M:exec(Tables, Props, Tx),
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
		%{badrpc, Msg} ->
		%	{_Error, Desc} = antidote_handler:handleBadRpc(Msg),
		%	%io:fwrite("[Err] ~p: ~p~n", [Error, Desc]),
		%	{error, Desc};
		Msg ->
			%io:fwrite("[????] ~p: ~p~n", [AQuery, Msg]),
			Msg
	end.

get_table_from_query(M, Props, TxId) ->
	TableName = M:table(Props),
	Tables = table:read_tables(TxId),
	Table = table:lookup(TableName, Tables),
	{Table, Tables}.

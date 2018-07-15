
-module(delete).

%% ====================================================================
%% API functions
%% ====================================================================
-export([exec/3, table/1, where/1]).

-define(REFINTEG_ERROR(TName, Value),
	io_lib:format("Cannot delete a parent row on table ~p: a foreign key constraint fails on deleting value ~p", [TName, Value])).

-include("parser.hrl").
-include("aql.hrl").
-include("types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

exec({Table, Tables}, Props, TxId) ->
	TName = table(Props),
	Condition = where(Props),
	Keys = where:scan(TName, Condition, TxId),
	lists:foreach(fun (Key) ->
		delete_cascade(Key, Table, Tables, TxId),
		ok = antidote:update_objects(crdt:ipa_update(Key, ipa:delete()), TxId)
	end, Keys).

table({TName, _Where}) -> TName.

where({_TName, Where}) -> Where.

%% ====================================================================
%% Internal functions
%% ====================================================================

delete_cascade(Key, Table, Tables, TxId) ->
	ok = delete_cascade_dependants(Key, Table, Tables, TxId).

delete_cascade_dependants(Key, Table, Tables, TxId) ->
	Dependants = cascade_dependants(Key, Table, Tables, TxId),
	DeleteUpdates = lists:foldl(fun({TName, Keys}, AccUpds) ->
		DepTable = table:lookup(TName, Tables),
		lists:foreach(fun(K) ->	delete_cascade(K, DepTable, Tables, TxId) end, Keys),
		lists:append(AccUpds, crdt:ipa_update(Keys, ipa:delete()))
	end, [], Dependants),
	case DeleteUpdates of
		[] ->
			ok;
		_Else ->
			antidote:update_objects(DeleteUpdates, TxId)
	end.

cascade_dependants(Key, Table, Tables, TxId) ->
	cascade_dependants(Key, Table, Tables, Tables, TxId, []).

cascade_dependants(Key, Table, AllTables, [{_TName, Table} | Tables], TxId, Acc) ->
	cascade_dependants(Key, Table, AllTables, Tables, TxId, Acc);
cascade_dependants(Key, Table, AllTables, [{{T1TName, _}, Table2} | Tables], TxId, Acc) ->
	TName = table:name(Table),
	Fks = table:shadow_columns(Table2),
	Refs = fetch_cascade(Key, TName, T1TName, AllTables, Fks, TxId, []),
	case Refs of
		error ->
			{PKey, _, _} = Key,
			ErrorMsg = ?REFINTEG_ERROR(TName, PKey),
			throw(lists:flatten(ErrorMsg));
		[] ->
			cascade_dependants(Key, Table, AllTables, Tables, TxId, Acc);
		_Else ->
			cascade_dependants(Key, Table, AllTables, Tables, TxId, lists:append(Acc, [{T1TName, Refs}]))
	end;
cascade_dependants(_Key, _Table, _AccTables, [], _TxId, Acc) -> Acc.

fetch_cascade(Key, TName, TDepName, Tables, [?T_FK(Name, _Type, TName, _Attr, ?CASCADE_TOKEN) | Fks], TxId, Acc)
	when length(Name) == 1 ->

	{PK, _, _} = Key,
	{_, _, Entries} = index:primary_index(TDepName, TxId),
	FilterDependants = lists:foldl(fun(Entry, DepAcc) ->
		{_FkName, {FkValue, _FkVersion}} = index:get_ref(Name, Entry),
		IsVisible = case utils:to_atom(FkValue) of
						PK -> element:is_visible(Entry, TDepName, Tables, TxId);
						_Else -> false
					end,
		case IsVisible of
			true -> lists:append(DepAcc, [index:entry_bobj(Entry)]);
			false -> DepAcc
		end
	end, [], Entries),

	case FilterDependants of
		[] ->
			fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, Acc);
		_Else ->
			fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, lists:append(Acc, FilterDependants))
	end;
fetch_cascade(Key, TName, TDepName, Tables, [?T_FK(Name, _Type, TName, _Attr, ?RESTRICT_TOKEN) | FKs], TxId, Acc)
	when length(Name) == 1 ->

	{PK, _, _} = Key,
	{_, _, Entries} = index:primary_index(TDepName, TxId),
	FilterDependants = lists:dropwhile(fun(Entry) ->
		{_FkName, {FkValue, _FkVersion}} = index:get_ref(Name, Entry),
		case utils:to_atom(FkValue) of
			PK -> not element:is_visible(Entry, TDepName, Tables, TxId);
			_Else -> true
		end
	end, Entries),

	case FilterDependants of
		[] ->
			fetch_cascade(Key, TName, TDepName, Tables, FKs, TxId, Acc);
		_Else ->
			error
	end;

fetch_cascade(Key, TName, TDepName, Tables, [_Fk | FKs], TxId, Acc) ->
	fetch_cascade(Key, TName, TDepName, Tables, FKs, TxId, Acc);
fetch_cascade(_Key, _TName, _TDepName, _Tables, [], _TxId, Acc) ->
	Acc.
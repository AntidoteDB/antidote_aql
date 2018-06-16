
-module(delete).

%% ====================================================================
%% API functions
%% ====================================================================
-export([exec/3, table/1, where/1]).

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
		case restrict_dependants(Key, Table, Tables, TxId) of
			[] ->
				delete_cascade(Key, Table, Tables, TxId),
				antidote:update_objects(crdt:ipa_update(Key, ipa:delete()), TxId);
			_Else ->
				{PKey, _, _} = Key,
				io:format("Error: Cannot delete a parent row: a foreign key constraint fails on deleting value ~p~n", [PKey])
		end
	end, Keys).

table({TName, _Where}) -> TName.

where({_TName, Where}) -> Where.

%% ====================================================================
%% Internal functions
%% ====================================================================

delete_cascade(Key, Table, Tables, TxId) ->
	TName = table:name(Table),
	{ok, [Data]} = antidote:read_objects(Key, TxId),
	case length(Data) of
		0 -> ok;
		1 -> ok;
		_Else ->
			Refs = table:dependants(TName, Tables),
			lists:foreach(fun({RefTName, RefCols}) ->
				lists:foreach(fun(?T_FK(FkName, FkType, _TName, CName, _DeleteRule)) ->
					Value = element:get(CName, types:to_crdt(FkType, ?IGNORE_OP), Data, Table),
					index:tag(RefTName, FkName, Value, ipa:delete_cascade(), TxId)
				end, RefCols)
			end, Refs),
			delete_cascade_dependants(Key, Table, Tables, TxId)
	end.

restrict_dependants(Key, Table, Tables, TxId) ->
	restrict_dependants(Key, Table, Tables, TxId, []).

restrict_dependants(Key, Table, [{_TName, Table} | Tables], TxId, Acc) ->
	restrict_dependants(Key, Table, Tables, TxId, Acc);
restrict_dependants(Key, Table, [{{T1TName, _}, Table2} | Tables], TxId, Acc) ->
	TName = table:name(Table),
	Cols = table:columns(Table2),
	Fks = foreign_keys:from_columns(Cols),
	Refs = fetch_restrict(Key, TName, Table, T1TName, Fks, TxId, []),
	case Refs of
		[] ->
			restrict_dependants(Key, Table, Tables, TxId, Acc);
		_Else ->
			restrict_dependants(Key, Table, Tables, TxId, lists:append(Acc, [{T1TName, Refs}]))
	end;
restrict_dependants(_Key, _Table, [], _TxId, Acc) -> Acc.

fetch_restrict(Key, TName, Table, TDepName, [?T_FK(Name, Type, TName, _Attr, ?RESTRICT_TOKEN) | Fks], TxId, Acc) ->
	{PK, _, _} = Key,
	Keys = where:scan(TDepName, ?PARSER_WILDCARD, TxId),
	FilterDependants = lists:dropwhile(fun(Key) ->
		{ok, [Record]} = antidote:read_objects(Key, TxId),
		case element:is_visible(Record, Table, TxId) of
			true ->
				case element:get(Name, types:to_crdt(Type, ?IGNORE_OP), Record, Table) of
					undefined -> true;
					Value -> utils:to_atom(Value) =/= PK
				end;
			false -> true
		end
	end, Keys),

	case FilterDependants of
		[] ->
			fetch_restrict(Key, TName, Table, TDepName, Fks, TxId, Acc);
		[Dependant | _Rest] ->
			fetch_restrict(Key, TName, Table, TDepName, Fks, TxId, lists:append(Acc, [Dependant]))
	end;
fetch_restrict(Key, TName, Table, TDepName, [_FK | FKs], TxId, Acc) -> fetch_restrict(Key, TName, Table, TDepName, FKs, TxId, Acc);
fetch_restrict(_Key, _TName, _Table, _TDepName, [], _TxId, Acc) -> Acc.

delete_cascade_dependants(Key, Table, Tables, TxId) ->
	Policy = table:policy(Table),
	DepLevel = crp:p_dep_level(Policy),
	case DepLevel of
		?ADD_WINS ->
			Dependants = cascade_dependants(Key, Table, Tables, TxId),
			DeleteUpdates = lists:foldl(fun({_TName, Keys}, AccUpds) ->
				lists:append(AccUpds, crdt:ipa_update(Keys, ipa:delete()))
			end, [], Dependants),
			case DeleteUpdates of
				[] -> ok;
				_Else ->
					antidote:update_objects(DeleteUpdates, TxId)
			end;
		_ -> ok
	end.

cascade_dependants(Key, Table, Tables, TxId) ->
	cascade_dependants(Key, Table, Tables, TxId, []).

cascade_dependants(Key, Table, [{_TName, Table} | Tables], TxId, Acc) ->
	cascade_dependants(Key, Table, Tables, TxId, Acc);
cascade_dependants(Key, Table, [{{T1TName, _}, Table2} | Tables], TxId, Acc) ->
	TName = table:name(Table),
	Cols = table:columns(Table2),
	Fks = foreign_keys:from_columns(Cols),
	Refs = fetch_cascade(Key, TName, Table, T1TName, Fks, TxId, []),
	case Refs of
		[] ->
			cascade_dependants(Key, Table, Tables, TxId, Acc);
		_Else ->
			cascade_dependants(Key, Table, Tables, TxId, lists:append(Acc, [{T1TName, Refs}]))
	end;
cascade_dependants(_Key, _Table, [], _TxId, Acc) -> Acc.

fetch_cascade(Key, TName, Table, TDepName, [?T_FK(Name, Type, TName, _Attr, ?CASCADE_TOKEN) | Fks], TxId, Acc) ->
	{PK, _, _} = Key,
	Keys = where:scan(TDepName, ?PARSER_WILDCARD, TxId),
	FilterDependants = lists:filter(fun(Key) ->
		{ok, [Record]} = antidote:read_objects(Key, TxId),
		case element:is_visible(Record, Table, TxId) of
			true ->
				case element:get(Name, types:to_crdt(Type, ?IGNORE_OP), Record, Table) of
					undefined -> true;
					Value -> utils:to_atom(Value) =:= PK
				end;
			false -> true
		end
	end, Keys),

	case FilterDependants of
		[] ->
			fetch_cascade(Key, TName, Table, TDepName, Fks, TxId, Acc);
		_Else ->
			fetch_cascade(Key, TName, Table, TDepName, Fks, TxId, lists:append(Acc, FilterDependants))
	end;
fetch_cascade(Key, TName, Table, TDepName, [_FK | FKs], TxId, Acc) -> fetch_restrict(Key, TName, Table, TDepName, FKs, TxId, Acc);
fetch_cascade(_Key, _TName, _Table, _TDepName, [], _TxId, Acc) -> Acc.
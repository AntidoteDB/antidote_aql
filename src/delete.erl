
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
		case has_restrict_dependants(Key, Table, [], Tables, TxId) of
			false ->
				delete_cascade(Key, Table, Tables, TxId),
				ok = antidote:update_objects(crdt:ipa_update(Key, ipa:delete()), TxId);
			true ->
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
	%TName = table:name(Table),
	{ok, [Data]} = antidote:read_objects(Key, TxId),
	case length(Data) of
		0 -> ok;
		1 -> ok;
		_Else ->
			%Refs = table:dependants(TName, Tables),
			%lists:foreach(fun({RefTName, RefCols}) ->
			%	lists:foreach(fun(?T_FK(FkName, FkType, _TName, CName, _DeleteRule)) ->
			%		Value = element:get(CName, types:to_crdt(FkType, ?IGNORE_OP), Data, Table),
			%		ok = index:tag(RefTName, FkName, Value, ipa:delete_cascade(), TxId)
			%	end, RefCols)
			%end, Refs),
			ok = delete_cascade_dependants(Key, Table, Tables, TxId)
	end.

has_restrict_dependants(Key, Table, AccTables, [{_TName, Table} = AccT | Tables], TxId) ->
	has_restrict_dependants(Key, Table, AccTables ++ [AccT], Tables, TxId);
has_restrict_dependants(Key, Table, AccTables, [{{_T1TName, _}, Table2} = AccT | Tables], TxId) ->
	Cols = table:columns(Table2),
	RestrictFks = foreign_keys:filter_restrict(Cols),
	RestrictRefs = has_restrict_fk(Key, Table, Table2, AccTables, RestrictFks, TxId),
	case RestrictRefs of
		false ->
			CascadeFks = foreign_keys:filter_cascade(Cols),
			CascadeRefs = has_restrict_fk(Key, Table, Table2, Tables, CascadeFks, TxId),
			case CascadeRefs of
				false -> has_restrict_dependants(Key, Table, AccTables ++ [AccT] ++ Tables, Tables, TxId);
				true -> true
			end;
		true ->	true
	end;
has_restrict_dependants(_Key, _Table, _AccTables, [], _TxId) -> false.

has_restrict_fk(Key, TName, RefTName, Tables, [?T_FK(FKName, FKType, FKTName, _Attr, ?RESTRICT_TOKEN) | Fks], TxId) ->
	case FKTName of
		TName ->
			{PK, _, _} = Key,
			Keys = where:scan(RefTName, ?PARSER_WILDCARD, TxId),
			FilterDependants = lists:dropwhile(fun(K) ->
				{ok, [Record]} = antidote:read_objects(K, TxId),
				case element:is_visible(Record, TName, Tables, TxId) of
					true ->
						case element:get(FKName, types:to_crdt(FKType, ?IGNORE_OP), Record, TName) of
							undefined -> true;
							Value -> utils:to_atom(Value) =/= PK
						end;
					false ->
						true
				end
			end, Keys),

			case FilterDependants of
				[] ->
					has_restrict_fk(Key, TName, RefTName, Tables, Fks, TxId);
				[_Dependant | _Rest] -> true
			end;
		_Else ->
			has_restrict_fk(Key, TName, RefTName, Tables, Fks, TxId)
	end;
%has_restrict_fk(_Key, _TName, _RefTName, _Tables, [], _TxId) ->
%	false.

has_restrict_fk(Key, TName, RefTName, Tables, [?T_FK(_FKName, _FKType, FKTName, _Attr, ?CASCADE_TOKEN) | Fks], TxId) ->
	case FKTName of
		TName ->
			Keys = where:scan(RefTName, ?PARSER_WILDCARD, TxId),
			FilterDependants = lists:dropwhile(fun(K) ->
				not has_restrict_dependants(K, table:lookup(RefTName, Tables), [], Tables, TxId)
			end, Keys),

			case FilterDependants of
				[] ->
					has_restrict_fk(Key, TName, RefTName, Tables, Fks, TxId);
				[_Dependant | _Rest] -> true
			end;
		_Else ->
			has_restrict_fk(Key, TName, RefTName, Tables, Fks, TxId)
	end;
has_restrict_fk(_Key, _Table, _RefTable, _Tables, [], _TxId) ->
	false.

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
				[] ->
					ok;
				_Else ->
					antidote:update_objects(DeleteUpdates, TxId)
			end;
		_ ->
			ok
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

fetch_cascade(Key, TName, TDepName, Tables, [?T_FK(Name, Type, TName, _Attr, ?CASCADE_TOKEN) | Fks], TxId, Acc) ->
	{PK, _, _} = Key,
	Keys = where:scan(TDepName, ?PARSER_WILDCARD, TxId),
	Table = table:lookup(TName, Tables),
	FilterDependants = lists:filter(fun(K) ->
		{ok, [Record]} = antidote:read_objects(K, TxId),
		case element:is_visible(Record, TName, Tables, TxId) of
			true ->
				case element:get(Name, types:to_crdt(Type, ?IGNORE_OP), Record, Table) of
					undefined -> true;
					Value -> utils:to_atom(Value) =:= PK
				end;
			false ->
				false
		end
	end, Keys),

	case FilterDependants of
		[] ->
			fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, Acc);
		_Else ->
			fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, lists:append(Acc, FilterDependants))
	end;
fetch_cascade(Key, TName, TDepName, Tables, [_FK | FKs], TxId, Acc) ->
	fetch_cascade(Key, TName, TDepName, Tables, FKs, TxId, Acc);
fetch_cascade(_Key, _TName, _TDepName, _Tables, [], _TxId, Acc) ->
	Acc.
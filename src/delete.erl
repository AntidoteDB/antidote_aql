
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
		%case has_restrict_dependants(Key, TName, Tables, Tables, TxId) of
		%	false ->
				delete_cascade(Key, Table, Tables, TxId),
				ok = antidote:update_objects(crdt:ipa_update(Key, ipa:delete()), TxId)
		%	true ->
		%		{PKey, _, _} = Key,
		%		ErrorMsg = io_lib:format("Cannot delete a parent row: a foreign key constraint fails on deleting value ~p", [PKey]),
		%		throw(lists:flatten(ErrorMsg))
		%end
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

has_restrict_dependants(Key, TName, AllTables, [{{TName, _}, _Table} | Tables], TxId) ->
	has_restrict_dependants(Key, TName, AllTables, Tables, TxId);
has_restrict_dependants(Key, TName, AllTables, [{{T1TName, _}, Table2} | Tables], TxId) ->
	Cols = table:columns(Table2),
	RestrictFks = foreign_keys:filter_restrict(Cols),
	RestrictRefs = has_restrict_fk(Key, TName, T1TName, AllTables, RestrictFks, TxId),
	case RestrictRefs of
		false ->
			CascadeFks = foreign_keys:filter_cascade(Cols),
			CascadeRefs = has_restrict_fk(Key, TName, T1TName, AllTables, CascadeFks, TxId),
			case CascadeRefs of
				false -> has_restrict_dependants(Key, TName, AllTables, Tables, TxId);
				true -> true
			end;
		true ->	true
	end;
has_restrict_dependants(_Key, _Table, _AllTables, [], _TxId) -> false.

has_restrict_fk(Key, TName, RefTName, Tables, [?T_FK(FKName, FKType, FKTName, _Attr, ?RESTRICT_TOKEN) | Fks], TxId) ->
	case FKTName of
		TName ->
			{PK, _, _} = Key,
			Keys = where:scan(RefTName, ?PARSER_WILDCARD, TxId),
			FilterDependants = lists:dropwhile(fun(K) ->
				{ok, [Record]} = antidote:read_objects(K, TxId),
				case element:is_visible(Record, RefTName, Tables, TxId) of
					true ->
						case element:get(FKName, types:to_crdt(FKType, ?IGNORE_OP), Record, RefTName) of
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
				not has_restrict_dependants(K, RefTName, Tables, Tables, TxId)
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
	%Policy = table:policy(Table),
	%DepLevel = crp:p_dep_level(Policy),
	%case DepLevel of
	%	?ADD_WINS ->
			Dependants = cascade_dependants(Key, Table, Tables, TxId),
			DeleteUpdates = lists:foldl(fun({TName, Keys}, AccUpds) ->
				lists:foreach(fun(Key2) ->
					delete_cascade(Key2, table:lookup(TName, Tables), Tables, TxId)
				end, Keys),
				lists:append(AccUpds, crdt:ipa_update(Keys, ipa:delete()))
			end, [], Dependants),
			case DeleteUpdates of
				[] ->
					ok;
				_Else ->
					antidote:update_objects(DeleteUpdates, TxId)
			end.
	%	_ ->
	%		ok
	%end.

cascade_dependants(Key, Table, Tables, TxId) ->
	cascade_dependants(Key, Table, Tables, Tables, TxId, []).

cascade_dependants(Key, Table, AllTables, [{_TName, Table} | Tables], TxId, Acc) ->
	cascade_dependants(Key, Table, AllTables, Tables, TxId, Acc);
cascade_dependants(Key, Table, AllTables, [{{T1TName, _}, Table2} | Tables], TxId, Acc) ->
	TName = table:name(Table),
	Cols = table:columns(Table2),
	Fks = foreign_keys:from_columns(Cols),
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

fetch_cascade(Key, TName, TDepName, Tables, [?T_FK(Name, Type, TName, _Attr, ?CASCADE_TOKEN) | Fks], TxId, Acc) ->
	{PK, _, _} = Key,
	Keys = where:scan(TDepName, ?PARSER_WILDCARD, TxId),
	DepTable = table:lookup(TDepName, Tables),
	FilterDependants = lists:filter(fun(K) ->
		{ok, [Record]} = antidote:read_objects(K, TxId),
		RefValue = utils:to_atom(element:get(Name, types:to_crdt(Type, ?IGNORE_OP), Record, DepTable)),
		case RefValue of
			PK -> element:is_visible(Record, TDepName, Tables, TxId);
			_Else -> false
		end
	end, Keys),

	case FilterDependants of
		[] ->
			fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, Acc);
		_Else ->
			fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, lists:append(Acc, FilterDependants))
	end;
fetch_cascade(Key, TName, TDepName, Tables, [?T_FK(Name, Type, TName, _Attr, ?RESTRICT_TOKEN) | FKs], TxId, Acc) ->
	{PK, _, _} = Key,
	Keys = where:scan(TDepName, ?PARSER_WILDCARD, TxId),
	DepTable = table:lookup(TDepName, Tables),
	FilterDependants = lists:dropwhile(fun(K) ->
		{ok, [Record]} = antidote:read_objects(K, TxId),
		RefValue = utils:to_atom(element:get(Name, types:to_crdt(Type, ?IGNORE_OP), Record, DepTable)),
		case RefValue of
			PK -> not element:is_visible(Record, TDepName, Tables, TxId);
			_Else -> true
		end
	end, Keys),

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
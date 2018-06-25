%% @author Joao
%% @doc @todo Add description to select.


-module(insert).

-define(NO_PK, none).

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([exec/3]).

-export([table/1,
				keys/2,
				values/1]).

-export([touch_cascade/5]).

exec({Table, Tables}, Props, TxId) ->
	Keys = keys(Props, Table),
	Values = values(Props),
	Keys1 = handle_defaults(Keys, Values, Table),
	AnnElement = element:new(Table),
	{ok, Element} = element:put(Keys1, Values, AnnElement),
	Element1 = element:build_fks(Element, TxId),
	ok = element:insert(Element1, TxId),
	%Pk = element:primary_key(Element1),
	%index:put(Pk, TxId),
	% update foreign key references
	%touch_cascade(Element1, Tables, TxId),
	%Fks = element:foreign_keys(foreign_keys:from_table(Table), Element1),
	%FksKV = read_fks(Fks, Tables, TxId, true),
	%lists:foreach(fun ({Fk, Data}) -> touch(Fk, Data, Tables, TxId) end, FksKV).
	touch_cascade(element, Element1, Table, Tables, TxId),
	ok.

table({TName, _Keys, _Values}) -> TName.

keys({_TName, Keys, _Values}, Table) ->
	case Keys of
		?PARSER_WILDCARD ->
			column:s_names(Table);
		_Else ->
			Keys
	end.

values({_TName, _Keys, Values}) -> Values.

touch_cascade(record, Record, Table, Tables, TxId) ->
	TName = table:name(Table),
	Fks = element:foreign_keys(foreign_keys:from_table(Table), Record, TName),
	FksKV = read_fks(Fks, Tables, TxId, true),
	lists:foreach(fun ({Fk, Data}) -> touch(Fk, Data, Tables, TxId) end, FksKV);
touch_cascade(element, Element, Table, Tables, TxId) ->
	Fks = element:foreign_keys(foreign_keys:from_table(Table), Element),
	FksKV = read_fks(Fks, Tables, TxId, true),
	lists:foreach(fun ({Fk, Data}) -> touch(Fk, Data, Tables, TxId) end, FksKV).

%% ====================================================================
%% Functions for inserts and updates
%% ====================================================================

read_fks(Fks, _Tables, TxId, false) ->
	lists:map(fun({_Col, {PTabName, _PTabAttr}, _DelRule, Value} = Fk) ->
		TKey = element:create_key(Value, PTabName),
		{ok, [Data]} = antidote:read_objects(TKey, TxId),
		{Fk, Data}
						end, Fks);
read_fks(Fks, Tables, TxId, true) ->
	lists:map(fun({_Col, {PTabName, _PTabAttr}, _DelRule, Value} = Fk) ->
		TKey = element:create_key(Value, PTabName),
		Table = table:lookup(PTabName, Tables),
		{ok, [Data]} = antidote:read_objects(TKey, TxId),
		case element:is_visible(Data, Table, TxId) of
			false ->
				throw(lists:concat(["Cannot find row ", Value, " in table ", PTabName]));
			_Else ->
				{Fk, Data}
		end
						end, Fks).

touch({_Col, {PTabName, _PTabAttr}, _DelRule, Value}, Data, Tables, TxId) ->
	TKey = element:create_key(Value, PTabName),
	Table = table:lookup(PTabName, Tables),
	Policy = table:policy(Table),
	case crp:p_dep_level(Policy) of
		?REMOVE_WINS -> ok;
		_Else -> antidote:update_objects(crdt:ipa_update(TKey, ipa:touch()), TxId)
	end,

	% touch cascade
	touch_cascade(Data, Table, Tables, TxId),
	% touch parents
	Fks = element:foreign_keys(foreign_keys:from_table(Table), Data, PTabName),
	FksKV = read_fks(Fks, Tables, TxId, false),
	lists:foreach(fun ({Fk, Data2}) -> touch(Fk, Data2, Tables, TxId) end, FksKV).

%% ====================================================================
%% Internal functions
%% ====================================================================

handle_defaults(Keys, Values, Table) ->
	if
		length(Keys) =:= length(Values) ->
			Keys;
		true ->
			Defaults = column:s_filter_defaults(Table),
			lists:filter(fun(Key) ->
				not maps:is_key(Key, Defaults)
		 	end, Keys)
	end.

touch_cascade(Data, Table, Tables, TxId) ->
	TName = table:name(Table),
	Refs = table:dependants(TName, Tables),
	lists:foreach(fun({RefTName, RefCols}) ->
		lists:foreach(fun(?T_FK(FkName, FkType, _TName, CName, _DeleteRule)) ->
			Value = element:get(CName, types:to_crdt(FkType, ?IGNORE_OP), Data, Table),
			ok = index:tag(RefTName, FkName, Value, ipa:touch_cascade(), TxId)
									end, RefCols)
								end, Refs).
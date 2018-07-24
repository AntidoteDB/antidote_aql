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
  	Element1 = element:set_version(Element, TxId),
	Element2 = element:build_fks(Element1, Tables, TxId),
	ok = element:insert(Element2, TxId),

	touch_cascade(element, Element2, Table, Tables, TxId),
	antidote:release_locks(lock_mgr_es, TxId),
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

read_fks(Fks, Tables, TxId, false) ->
	lists:map(fun({_Col, {PTabName, _PTabAttr}, _DelRule, Value} = Fk) ->
		%TKey = element:create_key(Value, PTabName),
		%{ok, [Data]} = antidote:read_objects(TKey, TxId),
        PTable = table:lookup(PTabName, Tables),
        Data = element:read_record(Value, PTable, TxId),
		{Fk, Data}
	end, Fks);
read_fks(Fks, Tables, TxId, true) ->
	lists:map(fun({_Col, {PTabName, _PTabAttr}, _DelRule, Value} = Fk) ->
		%TKey = element:create_key(Value, PTabName),
		%{ok, [Data]} = antidote:read_objects(TKey, TxId),
        PTable = table:lookup(PTabName, Tables),
        Data = element:read_record(Value, PTable, TxId),
		case element:is_visible(Data, PTabName, Tables, TxId) of
			false ->
				element:throwNoSuchRow(Value, PTabName);
			_Else ->
				{Fk, Data}
		end
	end, Fks).

touch({_Col, {PTabName, _PTabAttr}, _DelRule, Value}, Data, Tables, TxId) ->
	Table = table:lookup(PTabName, Tables),
    TKey = element:create_key_from_table(Value, Table, TxId),
	Policy = table:policy(Table),
	case crp:p_dep_level(Policy) of
		?REMOVE_WINS -> ok;
		_Else -> antidote:update_objects(crdt:ipa_update(TKey, ipa:touch()), TxId)
	end,

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

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
	Element2 = element:build_fks(Element1, TxId),
	ok = element:insert(Element2, TxId),

	touch_cascade(element, Element2, Table, Tables, TxId),
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
	lists:foreach(fun ({Fk, Entry}) -> touch(Fk, Entry, Tables, TxId) end, FksKV);
touch_cascade(entry, Entry, _Table, Tables, TxId) ->
    Fks = index:format_refs(Entry),
    FksKV = read_fks(Fks, Tables, TxId, true),
    lists:foreach(fun ({Fk, FkEntry}) -> touch(Fk, FkEntry, Tables, TxId) end, FksKV);
touch_cascade(element, Element, Table, Tables, TxId) ->
	Fks = element:foreign_keys(foreign_keys:from_table(Table), Element),
	FksKV = read_fks(Fks, Tables, TxId, true),
	lists:foreach(fun ({Fk, Entry}) -> touch(Fk, Entry, Tables, TxId) end, FksKV).

%% ====================================================================
%% Functions for inserts and updates
%% ====================================================================

read_fks(Fks, _Tables, TxId, false) ->
	lists:map(fun({?T_FK(_, _, PTabName, _, _), Value} = Fk) ->
		IndexEntry = index:p_keys(PTabName, {get, Value}, TxId),
		{Fk, IndexEntry}
	end, Fks);
read_fks(Fks, Tables, TxId, true) ->
	lists:map(fun({?T_FK(_, _, PTabName, _, _), Value} = Fk) ->
		IndexEntry = index:p_keys(PTabName, {get, Value}, TxId),
		case element:is_visible(IndexEntry, PTabName, Tables, TxId) of
			false ->
				ErrorMsg = io_lib:format("Cannot find row ~p in table ~p", [utils:to_atom(Value), PTabName]),
				throw(lists:flatten(ErrorMsg));
			_Else ->
				{Fk, IndexEntry}
		end
	end, Fks).

touch({?T_FK(_, _, PTabName, _, _), Value}, Entry, Tables, TxId) ->
	TKey = element:create_key(Value, PTabName),
	Table = table:lookup(PTabName, Tables),
	Policy = table:policy(Table),
	case crp:p_dep_level(Policy) of
		?REMOVE_WINS -> ok;
		_Else -> antidote:update_objects(crdt:ipa_update(TKey, ipa:touch()), TxId)
	end,

	% touch parents
	Fks = index:format_refs(Entry),
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

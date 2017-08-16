
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
		antidote:update_objects(crdt:ipa_update(Key, ipa:delete()), TxId),
		delete_cascade(Key, Table, Tables, TxId)
	end, Keys).

table({TName, _Where}) -> TName.

where({_TName, Where}) -> Where.

%% ====================================================================
%% Internal functions
%% ====================================================================

delete_cascade(Key, Table, Tables, TxId) ->
	TName = table:name(Table),
	{ok, [Data]} = antidote:read_objects(Key, TxId),
	Refs = table:dependants(TName, Tables),
	lists:foreach(fun({RefTName, RefCols}) ->
		lists:foreach(fun(?T_FK(FkName, FkType, _TName, CName)) ->
			Value = element:get(CName, types:to_crdt(FkType), Data, Table),
			index:tag(RefTName, FkName, Value, ipa:delete_cascade(), TxId)
		end, RefCols)
	end, Refs).

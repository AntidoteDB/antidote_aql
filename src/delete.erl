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

exec({_Table, _Tables}, Props, TxId) ->
	TName = table(Props),
	Condition = where(Props),
	Keys = where:scan(TName, Condition, TxId),
	lists:foreach(fun (Key) ->
		ok = antidote_handler:update_objects(crdt:ipa_update(Key, ipa:delete()), TxId)
	end, Keys).

table({TName, _Where}) -> TName.

where({_TName, Where}) -> Where.


%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. jun 2018 17:09
%%%-------------------------------------------------------------------
-module(secondary_indexes_SUITE).

-include_lib("aql.hrl").
-include_lib("parser.hrl").
-include_lib("types.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ct_aql.hrl").

-export([init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2,
  all/0]).

%% API
-export([insert_data_after/1, insert_data_before/1, bcounter_index/1]).

init_per_suite(Config) ->
  TestTable = "TableAIdx",
  TestTable2 = "TableBCIdx",
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TestTable,
    " (X VARCHAR PRIMARY KEY, Y INTEGER);"])),
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TestTable2,
    " (X VARCHAR PRIMARY KEY, Z COUNTER_INT CHECK (Z > 0));"])),
  lists:append(Config,
    [{table, TestTable},
     {bcounter_table, TestTable2},
     {indexed_col, "Y"},
     {bcounter_col, "Z"},
     {bound_greater, 0},
     {update_greater, lists:concat(["UPDATE ", TestTable2, " SET Z ~s WHERE X = ~s"])}]).

end_per_suite(Config) ->
  Config.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_, _) ->
  ok.

all() ->
  [
    insert_data_after,
    insert_data_before,
    bcounter_index
  ].

insert_data_after(Config) ->
  TestTable = ?value(table, Config),
  Column = ?value(indexed_col, Config),
  IdxName = lists:concat([Column, "Idx"]),

  {ok, [], _Tx} = tutils:create_index(IdxName, TestTable, Column),
  insert_data(TestTable),

  IndexData = tutils:read_index(TestTable, IdxName),
  ok = assert_index(IndexData, 4, [5, 10, 15, 20], [[a, e], [b, c], [d], [f, g]]).

insert_data_before(Config) ->
  TestTable = ?value(table, Config),
  Column = ?value(indexed_col, Config),
  IdxName = lists:concat([Column, "Idx"]),

  insert_data(TestTable),
  {ok, [], _Tx} = tutils:create_index(IdxName, TestTable, Column),

  IndexData = tutils:read_index(TestTable, IdxName),
  ok = assert_index(IndexData, 4, [5, 10, 15, 20], [[a, e], [b, c], [d], [f, g]]).

bcounter_index(Config) ->
  TestTable = ?value(bcounter_table, Config),
  Column = ?value(bcounter_col, Config),
  IdxName = lists:concat([Column, "Idx"]),

  {ok, [], _Tx} = tutils:create_index(IdxName, TestTable, Column),
  insert_data(TestTable),

  IndexData = tutils:read_index(TestTable, IdxName),
  ok = assert_index(IndexData, 4, [5, 10, 15, 20], [[a, e], [b, c], [d], [f, g]]),
  %ok = assert_index(IndexData, 4, [4, 9, 14, 19], [[a, e], [b, c], [d], [f, g]]),

  Keys = ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
  Bounds = [5, 10, 10, 15, 5, 20, 20],
  reset_counters(Keys, ?PARSER_GREATER, Bounds, Config).

%% ====================================================================
%% Internal functions
%% ====================================================================

insert_data(TableName) ->
  {ok, [], _Tx} = tutils:insert_single(TableName, "'a', 5"),
  {ok, [], _Tx} = tutils:insert_single(TableName, "'b', 10"),
  {ok, [], _Tx} = tutils:insert_single(TableName, "'c', 10"),
  {ok, [], _Tx} = tutils:insert_single(TableName, "'d', 15"),
  {ok, [], _Tx} = tutils:insert_single(TableName, "'e', 5"),
  {ok, [], _Tx} = tutils:insert_single(TableName, "'f', 20"),
  {ok, [], _Tx} = tutils:insert_single(TableName, "'g', 20").

assert_index(IndexData, _ExpLen, IdxValues, ExpEntries)
  when length(IdxValues) == length(ExpEntries) ->

  %?assertEqual(ExpLen, length(IndexData)),
  assert_multi(IndexData, IdxValues, ExpEntries).

assert_multi(IndexData, [Val | ValList], [ExpEntry | ExpEntryList]) ->
  {ok, Entry} = orddict:find(Val, IndexData),
  PKeys = lists:map(fun({Key, _Type, _Bucket}) -> Key end, ordsets:to_list(Entry)),

  ?assertEqual(ExpEntry, PKeys),

  assert_multi(IndexData, ValList, ExpEntryList);
assert_multi(_IndexData, [], []) -> ok.

update_key(?PARSER_GREATER) -> update_greater;
update_key(?PARSER_LESSER) -> update_smaller.

reset_counters([Key | Keys], Comp, [Bound | Bounds], Config) ->
  {InvBcA} = invert(Comp, Bound, Config),
  Updates = gen_reset_updates(Key, Comp, InvBcA),
  Query = ?format(update_key(Comp), Updates, Config),
  ct:log(info, lists:concat(["Reseting counters: ", Query])),
  {ok, [], _Tx} = tutils:aql(Query),
  reset_counters(Keys, Comp, Bounds, Config);
reset_counters([], _Comp, [], _Config) ->
  ok.

gen_reset_updates(Key, ?PARSER_GREATER, BcA) ->
  gen_reset_updates(Key, "DECREMENT", BcA);
gen_reset_updates(Key, ?PARSER_LESSER, BcA) ->
  gen_reset_updates(Key, "INCREMENT", BcA);
gen_reset_updates(Key, Op, BcA) ->
  [
    lists:concat([Op, " ", BcA]),
    lists:concat(["'", atom_to_list(Key), "'"])
  ].

invert(Comp, BcA, Config) ->
  {OffA} = offset(Comp, Config),
  InvBcA = bcounter:to_bcounter(none, BcA, OffA, Comp),
  {InvBcA}.

offset(?PARSER_GREATER, Config) ->
  OffA = ?value(bound_greater, Config),
  {OffA}.
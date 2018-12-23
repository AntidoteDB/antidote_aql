%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%% Created : 25. jun 2018 10:14
%%%-------------------------------------------------------------------
-module(rangequeries_SUITE).

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
-export([basic_range_queries/1, bcounter_range_queries/1]).

init_per_suite(Config) ->
  %aql:start(),
  TestTable = "TableA",
  TestTable2 = "TableBC",
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE UPDATE-WINS TABLE ", TestTable,
    " (X VARCHAR PRIMARY KEY, Y INTEGER);"])),
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE UPDATE-WINS TABLE ", TestTable2,
    " (X VARCHAR PRIMARY KEY, Z COUNTER_INT CHECK (Z > 0));"])),
  lists:append(Config,
    [{table, TestTable},
      {bcounter_table, TestTable2},
      {range_col, "Y"},
      {bcounter_col, "Z"},
      {bound_greater, 0},
      {update_greater, lists:concat(["UPDATE ", TestTable2, " SET Z = Z ~s WHERE X = ~s"])}]).

end_per_suite(Config) ->
  %aql:stop(),
  Config.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_, _) ->
  ok.

all() ->
  [
    basic_range_queries,
    bcounter_range_queries
  ].

basic_range_queries(Config) ->
  TestTable = ?value(table, Config),
  Column = ?value(range_col, Config),

  insert_data(TestTable),

  Greater = {Column, ?PARSER_GREATER, 5},
  LesserEq = {Column, ?PARSER_LEQ, 15},
  Equals = {Column, ?PARSER_EQUALITY, 20},
  Lesser = {Column, ?PARSER_LESSER, 10},
  NotEq = {Column, ?PARSER_NEQ, 20},

  Res1 = exec_query(conjunction, TestTable, Greater, LesserEq),
  ?assertEqual(3, length(Res1)),

  Res2 = exec_query(disjunction, TestTable, Equals, Lesser),
  ?assertEqual(4, length(Res2)),

  Res3 = exec_query(TestTable, NotEq),
  ?assertEqual(5, length(Res3)),

  Res4 = exec_query(conjunction, TestTable, NotEq, Equals),
  ?assertEqual(0, length(Res4)).

bcounter_range_queries(Config) ->
  TestTable = ?value(bcounter_table, Config),
  Column = ?value(bcounter_col, Config),

  insert_data(TestTable),

  Greater = {Column, ?PARSER_GREATER, 5},
  LesserEq = {Column, ?PARSER_LEQ, 15},
  Equals = {Column, ?PARSER_EQUALITY, 20},
  Lesser = {Column, ?PARSER_LESSER, 10},
  NotEq = {Column, ?PARSER_NEQ, 20},

  Res1 = exec_query(conjunction, TestTable, Greater, LesserEq),
  ?assertEqual(3, length(Res1)),

  Res2 = exec_query(disjunction, TestTable, Equals, Lesser),
  ?assertEqual(4, length(Res2)),

  Res3 = exec_query(TestTable, NotEq),
  ?assertEqual(5, length(Res3)),

  Res4 = exec_query(conjunction, TestTable, NotEq, Equals),
  ?assertEqual(0, length(Res4)),

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

prepare_range_query(Table, Comp) ->
  Concat = lists:concat(["SELECT * FROM ", Table, " WHERE ~s;"]),
  lists:flatten(io_lib:format(Concat, [to_string(Comp)])).
prepare_range_query(Table, LogicOp, Comp1, Comp2) ->
  Concat = lists:concat(["SELECT * FROM ", Table, " WHERE ~s ", LogicOp, " ~s;"]),
  lists:flatten(io_lib:format(Concat, [to_string(Comp1), to_string(Comp2)])).

exec_query(Table, Comp) ->
  Query = prepare_range_query(Table, Comp),
  {ok, [Res], _Tx} = tutils:aql(Query),
  Res.
exec_query(conjunction, Table, Comp1, Comp2) ->
  Query = prepare_range_query(Table, "AND", Comp1, Comp2),
  {ok, [Res], _Tx} = tutils:aql(Query),
  Res;
exec_query(disjunction, Table, Comp1, Comp2) ->
  Query = prepare_range_query(Table, "OR", Comp1, Comp2),
  {ok, [Res], _Tx} = tutils:aql(Query),
  Res.

to_string({Column, CompOp, Value}) ->
  lists:flatten(io_lib:format("~s ~s ~p", [Column, op_to_string(CompOp), Value])).
op_to_string(?PARSER_GREATER) -> ">";
op_to_string(?PARSER_GEQ) -> ">=";
op_to_string(?PARSER_LESSER) -> "<";
op_to_string(?PARSER_LEQ) -> "<=";
op_to_string(?PARSER_EQUALITY) -> "=";
op_to_string(?PARSER_NEQ) -> "<>".

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
  gen_reset_updates(Key, "-", BcA);
gen_reset_updates(Key, ?PARSER_LESSER, BcA) ->
  gen_reset_updates(Key, "+", BcA);
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
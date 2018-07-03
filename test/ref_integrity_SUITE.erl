%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%% Created : 02. jul 2018 17:06
%%%-------------------------------------------------------------------
-module(ref_integrity_SUITE).

-define(FK_POLICY_AW, "UPDATE-WINS").
-define(FK_POLICY_RW, "DELETE-WINS").
-define(DELETE_ERROR(Value),
  lists:flatten(
    io_lib:format("Cannot delete a parent row: a foreign key constraint fails on deleting value ~p", [Value]))).

-include_lib("aql.hrl").
-include_lib("parser.hrl").
-include_lib("types.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ct_aql.hrl").

-export([
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2,
  all/0]).

-export([
  dc_update_wins/1,
  dc_delete_wins/1,
  restrict_delete/1]).

%% API
-export([]).

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  Config.

init_per_testcase(dc_update_wins, Config) ->
  ok = create_tables("AW", ?FK_POLICY_AW),
  ok = insert_data(),
  Config;
init_per_testcase(dc_delete_wins, Config) ->
  ok = create_tables("AW", ?FK_POLICY_RW),
  ok = insert_data(),
  Config;
init_per_testcase(restrict_delete, Config) ->
  ok = create_restrict_tables("AW", ?FK_POLICY_RW),
  ok = insert_data(),
  Config.

end_per_testcase(_, _) ->
  {ok, [], _Tx} = tutils:aql("DELETE FROM TestRefA WHERE ID = 1"),
  ok.

all() ->
  [
    dc_update_wins,
    dc_delete_wins,
    restrict_delete
  ].

dc_update_wins(_Config) ->
  {ok, [Res1], _Tx} = tutils:select_all("TestRefA"),
  ?assertEqual(1, length(Res1)),

  %% Delete a key from TestRefA
  {ok, _, _Tx} = tutils:delete_by_key("TestRefA", "1"),
  tutils:assertState(false, "TestRefA", "1"),
  {ok, [Res2], _Tx} = tutils:select_all("TestRefB"),
  {ok, [Res3], _Tx} = tutils:select_all("TestRefC"),
  ?assertEqual(0, length(Res2)),
  ?assertEqual(0, length(Res3)),

  %% Insert again the deleted key
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefA VALUES (1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefB VALUES (1, 1)"),
  {ok, [Res4], _Tx} = tutils:select_all("TestRefB"),
  ?assertEqual(1, length(Res4)),
  tutils:assertState(false, "TestRefC", "1"),
  tutils:assertState(false, "TestRefC", "1").

dc_delete_wins(_Config) ->
  {ok, [Res1], _Tx} = tutils:select_all("TestRefA"),
  ?assertEqual(1, length(Res1)),

  %% Delete a key from TestRefA
  {ok, _, _Tx} = tutils:delete_by_key("TestRefA", "1"),
  tutils:assertState(false, "TestRefA", "1"),
  {ok, [Res2], _Tx} = tutils:select_all("TestRefB"),
  {ok, [Res3], _Tx} = tutils:select_all("TestRefC"),
  ?assertEqual(0, length(Res2)),
  ?assertEqual(0, length(Res3)),

  %% Insert again the deleted key
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefA VALUES (1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefB VALUES (1, 1)"),
  {ok, [Res4], _Tx} = tutils:select_all("TestRefB"),
  ?assertEqual(1, length(Res4)),
  tutils:assertState(false, "TestRefC", "1"),
  tutils:assertState(false, "TestRefC", "1").

restrict_delete(_Config) ->
  {error, Msg1, _} = tutils:aql("DELETE FROM TestRefA WHERE ID = 1"),
  ?assertEqual(?DELETE_ERROR('1'), Msg1),

  %% Delete a key from TestRefC
  {ok, _, _Tx} = tutils:delete_by_key("TestRefC", "1"),
  tutils:assertState(false, "TestRefC", "1"),
  {error, Msg2, _} = tutils:aql("DELETE FROM TestRefA WHERE ID = 1"),
  ?assertEqual(?DELETE_ERROR('1'), Msg2),

  %% Delete the second key from TestRefC
  {ok, _, _Tx} = tutils:delete_by_key("TestRefC", "2"),
  tutils:assertState(false, "TestRefC", "2"),
  {Res3, _, _} = tutils:aql("DELETE FROM TestRefA WHERE ID = 1"),
  ?assertEqual(ok, Res3).

%% ====================================================================
%% Internal functions
%% ====================================================================

create_tables(TablePolicy, DepPolicy) ->
  {ok, [], _Tx} = tutils:create_single_table("TestRefA", TablePolicy),
  {ok, [], _Tx} = tutils:create_dc_fk_table("TestRefB", "TestRefA", TablePolicy, DepPolicy),
  {ok, [], _Tx} = tutils:create_dc_fk_table("TestRefC", "TestRefB", TablePolicy, DepPolicy),
  ok.

create_restrict_tables(TablePolicy, DepPolicy) ->
  {ok, [], _Tx} = tutils:create_single_table("TestRefA", TablePolicy),
  {ok, [], _Tx} = tutils:create_dc_fk_table("TestRefB", "TestRefA", TablePolicy, DepPolicy),
  {ok, [], _Tx} = tutils:create_fk_table("TestRefC", "TestRefB", TablePolicy, DepPolicy),
  ok.

insert_data() ->
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefA VALUES (1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefB VALUES (1, 1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefB VALUES (2, 1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefC VALUES (1, 1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO TestRefC VALUES (2, 2)"),
  ok.
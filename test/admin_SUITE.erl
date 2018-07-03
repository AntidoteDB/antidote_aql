
-module(admin_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([init_per_suite/1,
          end_per_suite/1,
          init_per_testcase/2,
          end_per_testcase/2,
          all/0]).

-export([show_tables/1,
        show_index/1,
        show_indexes/1]).

%% ====================================================================
%% CT config functions
%% ====================================================================

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  Config.

init_per_testcase(_Case, Config) ->
  Config.

end_per_testcase(_, _) ->
  ok.

all() ->
  [show_tables, show_index, show_indexes].

%% ====================================================================
%% Test functions
%% ====================================================================

show_tables(_Config) ->
  tutils:create_single_table("ShowTablesTest", "AW"),
  {ok, [Res], _Tx} = tutils:aql("SHOW TABLES"),
  io:fwrite("~p~n", [Res]),
  ?assertEqual(true, is_list(Res)).

show_index(_Config) ->
  tutils:create_single_table("ShowIndexTest", "AW"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO ShowIndexTest VALUES (1)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO ShowIndexTest VALUES (2)"),
  {ok, [], _Tx} = tutils:aql("INSERT INTO ShowIndexTest VALUES (3)"),
  {ok, [Index], _Tx} = tutils:aql("SHOW INDEX FROM ShowIndexTest"),
  ?assertEqual(3, length(Index)).

show_indexes(_Config) ->
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ShowIndexesTest",
    " (Pk VARCHAR PRIMARY KEY, X INTEGER, Y VARCHAR, Z BOOLEAN);"])),
  {ok, [], _Tx} = tutils:aql("CREATE INDEX IndexTestA ON ShowIndexesTest (X)"),
  {ok, [], _Tx} = tutils:aql("CREATE INDEX IndexTestB ON ShowIndexesTest (Y)"),
  {ok, [], _Tx} = tutils:aql("CREATE INDEX IndexTestB ON ShowIndexesTest (Z)"),
  {ok, [Indexes], _Tx} = tutils:aql("SHOW INDEXES FROM ShowIndexesTest"),
  ?assertEqual(3, length(Indexes)).
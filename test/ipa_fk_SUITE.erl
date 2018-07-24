-module(ipa_fk_SUITE).

-include_lib("aql.hrl").
-include_lib("parser.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SHADOW_AB, {[{'FkC', 'FkB'}, {'FkB', 'FkA'}], ?CRDT_INTEGER}).
-define(SHADOW_ABC, {[{'FkD', 'FkC'}, {'FkC', 'FkB'}, {'FkB', 'FkA'}], ?CRDT_INTEGER}).
-define(SHADOW_BC, {[{'FkD', 'FkC'}, {'FkC', 'FkB'}], ?CRDT_INTEGER}).

-define(FK_POLICY_AW, "UPDATE-WINS").
-define(FK_POLICY_RW, "DELETE-WINS").

-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

-export([indirect_foreign_keys/1,
    %touch_cascade/1,
    insert_multilevel/1,
    delete_basic/1, delete_multilevel/1,
    create_table_fail/1,
    reference_deleted_fail/1]).

init_per_suite(Config) ->
    {ok, [], _Tx} = tutils:create_single_table("FkA", "AW"),
    {ok, [], _Tx} = tutils:create_dc_fk_table("FkB", "FkA", "AW", ?FK_POLICY_RW),
    {ok, [], _Tx} = tutils:create_dc_fk_table("FkC", "FkB", "AW", ?FK_POLICY_RW),
    {ok, [], _Tx} = tutils:create_dc_fk_table("FkD", "FkC", "AW", ?FK_POLICY_RW),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkA VALUES (1)"),
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkB VALUES (1, 1)"),
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkB VALUES (2, 1)"),
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkC VALUES (1, 1)"),
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkC VALUES (2, 1)"),
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkD VALUES (1, 1)"),
    {ok, [], _Tx} = tutils:aql("INSERT INTO FkD VALUES (2, 1)"),
    Config.

end_per_testcase(_, _) ->
    {ok, [], _Tx} = tutils:aql("DELETE FROM FkA WHERE ID = 1"),
    ok.

all() ->
    [
        indirect_foreign_keys,
        %touch_cascade,
        insert_multilevel,
        delete_basic, delete_multilevel,
        create_table_fail,
        reference_deleted_fail
    ].

indirect_foreign_keys(_Config) ->
    KeyC = element:create_key('1', 'FkC'),
    KeyD = element:create_key('1', 'FkD'),
    [ResC, ResD] = tutils:read_keys([KeyC, KeyD]),
    ?assertMatch({1, _}, proplists:get_value(?SHADOW_AB, ResC)),
    ?assertMatch({1, _}, proplists:get_value(?SHADOW_ABC, ResD)),
    ?assertMatch({1, _}, proplists:get_value(?SHADOW_BC, ResD)).

create_table_fail(_Config) ->
    % cannot create table that points to a non-existant table
    {_, [{error, Msg1}], _} = tutils:create_dc_fk_table("FkETest", "FkFTest"),
    ?assertEqual("No such table: 'FkFTest'", Msg1),
    % cannot create a table that points to a non-existant column
    {_, [{error, Msg2}], _} = tutils:create_dc_fk_table("FkETest", "FkA", "ABC", "AW", "DELETE-WINS"),
    ?assertEqual("Column 'ABC' does not exist in table 'FkA'", Msg2),
    % cannot create a table that points to a non-primary key column
    {_, [{error, Msg3}], _} = tutils:create_dc_fk_table("FkETest", "FkB", "FkA", "AW", "DELETE-WINS"),
    ?assertEqual("Foreign keys can only reference unique columns", Msg3).

%%touch_cascade(_Config) ->
%%  tutils:assertExists(index:tag_key('FkB', [{'FkB', 'FkA'}])),
%%  tutils:assertExists(index:tag_key('FkC', [{'FkC', 'FkB'}])),
%%  {TypelessCAB, _TypeCAB} = ?SHADOW_AB,
%%  tutils:assertExists(index:tag_key('FkC', TypelessCAB)),
%%  tutils:assertExists(index:tag_key('FkD', [{'FkD', 'FkC'}])),
%%  {TypelessDABC, _TypeDABC} = ?SHADOW_ABC,
%%  tutils:assertExists(index:tag_key('FkD', TypelessDABC)),
%%  {TypelessDBC, _TypeDBC} = ?SHADOW_BC,
%%  tutils:assertExists(index:tag_key('FkD', TypelessDBC)).

insert_multilevel(_Config) ->
    %bottom level insert
    tutils:assertState(true, "FkA", "1"),
    tutils:assertState(true, "FkB", "1"),
    tutils:assertState(true, "FkB", "2"),
    tutils:assertState(true, "FkC", "1"),
    tutils:assertState(true, "FkC", "2"),
    % middle level insert; the following FkC records
    % will not be visible because a new version for
    % ID = 1 from FkB has been created
    tutils:aql("INSERT INTO FkB VALUES (1, 1)"),
    tutils:assertState(false, "FkC", "1"),
    tutils:assertState(false, "FkC", "2").

delete_basic(_Config) ->
    {ok, [], _Tx} = tutils:delete_by_key("FkA", "1"),
    tutils:assertState(false, "FkA", "1"),
    tutils:assertState(false, "FkB", "1").

delete_multilevel(_Config) ->
    {ok, [], _Tx} = tutils:delete_by_key("FkA", "1"),
    tutils:assertState(false, "FkA", "1"),
    tutils:assertState(false, "FkB", "1"),
    tutils:assertState(false, "FkB", "2"),
    tutils:assertState(false, "FkC", "1"),
    tutils:assertState(false, "FkB", "2"),
    tutils:assertState(false, "FkD", "1").

reference_deleted_fail(_Config) ->
    {ok, [], _Tx} = tutils:delete_by_key("FkA", "1"),
    {_, [{error, Msg1}], _} = tutils:aql("INSERT INTO FkB VALUES (2, 1)"),
    {_, [{error, Msg2}], _} = tutils:aql("INSERT INTO FkC VALUES (1, 1)"),

    ?assertEqual("Cannot find row '1' in table 'FkA'", Msg1),
    ?assertEqual("Cannot find row '1' in table 'FkB'", Msg2).

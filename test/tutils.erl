

-module(tutils).

-define(TEST_SERVER, 'antidote@127.0.0.1').

-include_lib("eunit/include/eunit.hrl").
-include("types.hrl").

-export([aql/1, aql/2,
          create_single_table/2,
          create_fk_table/2, create_fk_table/4, create_fk_table/5,
          create_dc_fk_table/2, create_dc_fk_table/4, create_dc_fk_table/5,
          create_index/3,
          insert_single/2,
          delete_by_key/2,
          read_keys/4, read_keys/3, read_keys/1, read_index/2,
          print_state/2,
          select_all/1]).

-export([assertState/3,
          assertExists/1, assertExists/2,
          assertNotExists/1, assertNotExists/2,
          assert_table_policy/2]).

aql(Aql) ->
  aql(Aql, undefined).

aql(Aql, Tx) ->
  ct:log(info, lists:concat(["Query: ", Aql])),
  aqlparser:parse({str, Aql}, ?TEST_SERVER, Tx).

create_single_table(Name, TablePolicy) ->
  Query = ["CREATE ", TablePolicy, " TABLE ", Name, " (ID INT PRIMARY KEY)"],
  aql(lists:concat(Query)).

create_fk_table(Name, Pointer) ->
  create_fk_table(Name, Pointer, "ID", "AW", "UPDATE-WINS").
create_fk_table(Name, Pointer, TablePolicy, FK_Policy) ->
  create_fk_table(Name, Pointer, "ID", TablePolicy, FK_Policy).
create_dc_fk_table(Name, Pointer) ->
  create_dc_fk_table(Name, Pointer, "ID", "AW", "UPDATE-WINS").
create_dc_fk_table(Name, Pointer, TablePolicy, FK_Policy) ->
  create_dc_fk_table(Name, Pointer, "ID", TablePolicy, FK_Policy).

create_fk_table(Name, TPointer, CPointer, TablePolicy, FK_Policy) ->
  Query = ["CREATE ", TablePolicy, " TABLE ", Name,
    " (ID INT PRIMARY KEY, ", TPointer, " INT FOREIGN KEY ", FK_Policy, " REFERENCES ",
    TPointer, "(", CPointer, "))"],
  aql(lists:concat(Query)).

create_dc_fk_table(Name, TPointer, CPointer, TablePolicy, FK_Policy) ->
  Query = ["CREATE ", TablePolicy," TABLE ", Name,
    " (ID INT PRIMARY KEY, ", TPointer, " INT FOREIGN KEY ", FK_Policy, " REFERENCES ",
    TPointer, "(", CPointer, ") ",
    "ON DELETE CASCADE)"],
  aql(lists:concat(Query)).

create_index(IndexName, Table, Column) ->
  Query = ["CREATE INDEX ", IndexName, " ON ", Table, " (", Column, ")"],
  aql(lists:concat(Query)).

insert_single(TName, ID) ->
  Query = lists:concat(["INSERT INTO ", TName, " VALUES (", ID, ");"]),
  aql(Query).

delete_by_key(TName, Key) ->
  Query = ["DELETE FROM ", TName, " WHERE ID = ", Key],
  aql(lists:concat(Query)).

assertState(State, TName, Key) ->
  %AQLKey = element:create_key(Key, TName),
  {ok, TxId} = antidote:start_transaction(?TEST_SERVER),
  Tables = table:read_tables(TxId),
  IndexEntry = index:p_keys(TName, {get, Key}, TxId),
  %{ok, [Res]} = antidote:read_objects(AQLKey, TxId),
  Actual = element:is_visible(IndexEntry, TName, Tables, TxId),
  antidote:commit_transaction(TxId),
  %ct:log(info, lists:concat(["State: ", State])),
  %ct:log(info, lists:concat(["Actual: ", Actual])),
  ?assertEqual(State, Actual).

print_state(TName, Key) ->
  TNameAtom = utils:to_atom(TName),
  AQLKey = element:create_key(Key, TNameAtom),
  {ok, TxId} = antidote:start_transaction(?TEST_SERVER),
  Tables = table:read_tables(TxId),
  Table = table:lookup(TName, Tables),
  {ok, [Data]} = antidote:read_objects(AQLKey, TxId),
  io:fwrite("Tags for ~p(~p)~nData: ~p~n", [TNameAtom, Key, Data]),
  lists:foreach(fun(?T_FK(FkName, FkType, _, _, _)) ->
    FkValue = element:get(foreign_keys:to_cname(FkName), types:to_crdt(FkType, ignore), Data, Table),
    Tag = index:tag_read(TNameAtom, FkName, FkValue, TxId),
    io:fwrite("Tag(~p): ~p -> ~p~n", [FkValue, index:tag_name(TNameAtom, FkName), Tag])
  end, table:shadow_columns(Table)),
  io:fwrite("Final: ~p~n", [element:is_visible(Data, TName, Tables, TxId)]),
antidote:commit_transaction(TxId).

select_all(TName) ->
  aql(lists:concat(["SELECT * FROM ", TName])).

assert_table_policy(Expected, TName) ->
  TNameAtom = utils:to_atom(TName),
  {ok, TxId} = antidote:start_transaction(?TEST_SERVER),
  Table = table:lookup(TNameAtom, TxId),
  antidote:commit_transaction(TxId),
  ?assertEqual(Expected, table:policy(Table)).

assertExists(TName, Key) ->
  assertExists(element:create_key(Key, TName)).

assertExists(Key) ->
  {ok, Ref} = antidote:start_transaction(?TEST_SERVER),
  {ok, [Res]} = antidote:read_objects(Key, Ref),
  antidote:commit_transaction(Ref),
  ?assertNotEqual([], Res).

assertNotExists(TName, Key) ->
  assertNotExists(element:create_key(Key, TName)).

assertNotExists(Key) ->
  {ok, Ref} = antidote:start_transaction(?TEST_SERVER),
  {ok, [Res]} = antidote:read_objects(Key, Ref),
  antidote:commit_transaction(Ref),
  ?assertEqual([], Res).

read_keys(Table, IdName, ID, Keys, Tx) ->
  Join = join_keys(Keys, []),
  Query = ["SELECT ", Join, " FROM ", Table, " WHERE ", IdName, " = ", ID],
  {ok, [Res], _Tx} = aql(lists:concat(Query), Tx),
  case Res of
    [] -> [];
    [Content] -> lists:map(fun({_k, V}) -> V end, Content)
  end.

read_keys(Table, IdName, ID, Keys) ->
  read_keys(Table, IdName, ID, Keys, undefined).

read_keys(Table, ID, Keys) ->
  read_keys(Table, "ID", ID, Keys, undefined).

read_keys(Keys) ->
  {ok, Ref} = antidote:start_transaction(?TEST_SERVER),
  {ok, Res} = antidote:read_objects(Keys, Ref),
  antidote:commit_transaction(Ref),
  Res.

read_index(TName, IndexName) ->
  {ok, Ref} = antidote:start_transaction(?TEST_SERVER),
  IndexData = index:s_keys_formatted(TName, IndexName, Ref),
  antidote:commit_transaction(Ref),
  IndexData.

join_keys([Key | Keys], []) ->
  join_keys(Keys, Key);
join_keys([Key | Keys], Acc) ->
  NewAcc = lists:concat([Acc, ", ", Key]),
  join_keys(Keys, NewAcc);
join_keys([], Acc) -> Acc.

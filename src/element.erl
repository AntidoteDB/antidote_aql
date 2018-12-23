-module(element).

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(CRDT_TYPE, antidote_crdt_map_go).
-define(EL_ANON, none).
-define(STATE, '#st').
-define(STATE_TYPE, antidote_crdt_register_mv).
-define(VERSION, '#version').
-define(VERSION_TYPE, antidote_crdt_register_lww).

-export([primary_key/1, set_primary_key/2,
  foreign_keys/1, foreign_keys/2, foreign_keys/3,
  attributes/1,
  data/1,
  table/1,
  parents/4]).

-export([create_key/2, create_key/3,
  create_key_from_data/2, create_key_from_table/3,
  st_key/0, version_key/0, is_visible/3, is_visible/4]).

-export([new/1, new/2,
  put/3, set_version/2, build_fks/2, build_fks/3,
  get/2, get/3, get/4,
  get_by_name/2,
  insert/1, insert/2, delete/2,
  read_record/3, shared_read/3, exclusive_read/3]).

-export([throwNoSuchRow/2]).

%% ====================================================================
%% Property functions
%% ====================================================================

ops({_BObj, _Table, Ops, _Data}) -> Ops.
set_ops({BObj, Table, _Ops, Data}, Ops) -> ?T_ELEMENT(BObj, Table, Ops, Data).

primary_key({BObj, _Table, _Ops, _Data}) -> BObj.
set_primary_key({_BObj, Table, Ops, Data}, BObj) -> ?T_ELEMENT(BObj, Table, Ops, Data).

foreign_keys(Element) ->
  foreign_keys:from_columns(attributes(Element)).

attributes(Element) ->
  Table = table(Element),
  table:columns(Table).

data({_BObj, _Table, _Ops, Data}) -> Data.
set_data({BObj, Table, Ops, _Data}, Data) -> ?T_ELEMENT(BObj, Table, Ops, Data).

table({_BObj, Table, _Ops, _Data}) -> Table.

%% ====================================================================
%% Utils functions
%% ====================================================================

create_key({Key, Type, Bucket}, _TName) ->
  {Key, Type, Bucket};
create_key(Key, TName) ->
  KeyAtom = utils:to_atom(Key),
  crdt:create_bound_object(KeyAtom, ?CRDT_TYPE, TName).

create_key(Key, TName, Prefix)
  when is_integer(Prefix) andalso is_atom(TName) ->
  KeyAtom = utils:to_atom(Key),
  Bucket = lists:flatten(io_lib:format("~p@~s", [Prefix, TName])),
  BucketAtom = utils:to_atom(Bucket),
  crdt:create_bound_object(KeyAtom, ?CRDT_TYPE, BucketAtom).

create_key_from_table({_K, _T, _B} = Key, _Table, _TxId) ->
  Key;
create_key_from_table(PKey, Table, TxId) ->
  TName = table:name(Table),
  PartCol = table:partition_col(Table),
  case PartCol of
    [_] ->
      Index = index:p_keys(TName, TxId),
      {_, BObj} = lists:keyfind(utils:to_atom(PKey), 1, Index),
      BObj;
    undefined ->
      create_key(PKey, TName)
  end.

create_key_from_data(Data, Table)
  when is_list(Data) andalso ?is_table(Table) ->
  [?T_COL(PkName, PkType, _PkConst)] = column:s_primary_key(Table),
  Key = get(PkName, types:to_crdt(PkType, ignore), Data, Table),
  TName = table:name(Table),

  PartCol = table:partition_col(Table),
  case PartCol of
    [Col] ->
      ?T_COL(ColName, ColType, _ColConst) = column:s_get(Table, Col),
      Value = get(ColName, types:to_crdt(ColType, ignore), Data, Table),
      Prefix = utils:to_hash(Value),
      create_key(Key, TName, Prefix);
    undefined ->
      create_key(Key, TName)
  end.

st_key() ->
  ?MAP_KEY(?STATE, ?STATE_TYPE).

version_key() ->
  ?MAP_KEY(?VERSION, ?VERSION_TYPE).

explicit_state(Data, Rule) ->
  Value = proplists:get_value(st_key(), Data),
  case Value of
    undefined ->
      throw("No explicit state found");
    _Else ->
      ipa:status(Rule, Value)
  end.

is_visible(Element, Tables, TxId) when is_tuple(Element) ->
  Data = data(Element),
  TName = table:name(table(Element)),
  is_visible(Data, TName, Tables, TxId).

is_visible([], _TName, _Tables, _TxId) -> false;
is_visible(Data, TName, Tables, TxId) ->
  Table = table:lookup(TName, Tables),
  Policy = table:policy(Table),
  Rule = crp:get_rule(Policy),
  ExplicitState = explicit_state(Data, Rule),
  ipa:is_visible(ExplicitState) andalso
    implicit_state(Table, Data, Tables, TxId).

throwNoSuchColumn(ColName, TableName) ->
  MsgFormat = io_lib:format("Column ~p does not exist in table ~p", [ColName, TableName]),
  throw(lists:flatten(MsgFormat)).

throwNoSuchRow(Key, TableName) ->
  MsgFormat = io_lib:format("Cannot find row ~p in table ~p", [utils:to_atom(Key), TableName]),
  throw(lists:flatten(MsgFormat)).

%% ====================================================================
%% API functions
%% ====================================================================

new(Table) when ?is_table(Table) ->
  new(?EL_ANON, Table).

new(Key, Table) ->
  Bucket = table:name(Table),
  BoundObject = create_key(Key, Bucket),
  StateOp = crdt:field_map_op(st_key(), crdt:assign_lww(ipa:new())),
  Ops = [StateOp],
  Element = ?T_ELEMENT(BoundObject, Table, Ops, []),
  load_defaults(Element).

load_defaults(Element) ->
  Columns = attributes(Element),
  Defaults = column:s_filter_defaults(Columns),
  maps:fold(fun(CName, Column, Acc) ->
    {?DEFAULT_TOKEN, Value} = column:constraint(Column),
    Constraint = {?DEFAULT_TOKEN, Value},
    append(CName, Value, column:type(Column), Constraint, Acc)
  end, Element, Defaults).

put([Key | OKeys], [Value | OValues], Element) ->
  utils:assert_same_size(OKeys, OValues, "Illegal number of keys and values"),
  Res = put(Key, Value, Element),
  put(OKeys, OValues, Res);
put([], [], Element) ->
  {ok, Element};
put(ColName, Value, Element) ->
  ColSearch = maps:get(ColName, attributes(Element)),
  case ColSearch of
    {badkey, _} ->
      Table = table(Element),
      TName = table:name(Table),
      throwNoSuchColumn(ColName, TName);
    Col ->
      ColType = column:type(Col),
      Constraint = column:constraint(Col),
      Element1 = set_if_primary(Col, Value, Element),
      Element2 = set_if_partition(Col, Value, Element1),
      append(ColName, Value, ColType, Constraint, Element2)
  end.

set_if_primary(Col, Value, Element) ->
  case column:is_primary_key(Col) of
    true ->
      ?BOUND_OBJECT(_Key, _Type, Bucket) = primary_key(Element),
      set_primary_key(Element, create_key(Value, Bucket));
    _Else ->
      Element
  end.

set_if_partition(?T_COL(Col, _, _), Value, Element) ->
  Table = table(Element),
  case table:partition_col(Table) of
    [Col] ->
      ?BOUND_OBJECT(Key, _Type, _Bucket) = primary_key(Element),
      TName = table:name(Table),
      HashValue = utils:to_hash(Value),
      set_primary_key(Element, create_key(Key, TName, HashValue));
    _ ->
      Element
  end.

set_version(Element, TxId) ->
  VersionKey = version_key(),
  CurrOps = ops(Element),
  ElemData = data(Element),

  Key = primary_key(Element),
  {ok, [CurrData]} = antidote_handler:read_objects(Key, TxId),
  Version = case CurrData of
              [] -> 1;
              _Else ->
                proplists:get_value(VersionKey, CurrData) + 1
            end,

  VersionOp = crdt:assign_lww(Version),

  Element1 = set_data(Element, lists:append(ElemData, [{VersionKey, Version}])),
  set_ops(Element1, utils:proplists_upsert(VersionKey, VersionOp, CurrOps)).

build_fks(Element, Tables, TxId) ->
  Data = data(Element),
  Table = table(Element),
  Parents = parents(Data, Table, Tables, TxId),
  build_fks(Element, Parents).

build_fks(Element, Parents) ->
  Table = table(Element),
  Fks = table:shadow_columns(Table),
  lists:foldl(fun(?T_FK(FkName, _, FkTable, FkColName, _), AccElement) ->
    case length(FkName) of
      1 ->
        [{_, ParentId}] = FkName,
        {Parent, _} = dict:fetch({FkTable, ParentId}, Parents),
        Value = get_by_name(foreign_keys:to_cname(FkColName), Parent),
        ParentVersion = get_by_name(?VERSION, Parent),
        append(FkName, {Value, ParentVersion}, ?AQL_VARCHAR, ?IGNORE_OP, AccElement);
      _Else ->
        %[{_, ParentId} | ParentCol] = FkName,
        %Parent = dict:fetch({FkTable, ParentId}, Parents),
        %Value = get_by_name(ParentCol, Parent),
        %append(FkName, Value, ?AQL_VARCHAR, ?IGNORE_OP, AccElement)
        AccElement
    end
  end, Element, Fks).

parents(Data, Table, Tables, TxId) ->
  Fks = table:shadow_columns(Table),
  lists:foldl(fun(?T_FK(Name, Type, TTName, _, _), Dict) ->
    case Name of
      [ShCol] ->
        {_FkTable, FkName} = ShCol,
        Value = get(FkName, types:to_crdt(Type, ?IGNORE_OP), Data, Table),
        RefTable = table:lookup(TTName, Tables),
        Parent = shared_read(Value, RefTable, TxId),
        case element:is_visible(Parent, TTName, Tables, TxId) of
          false ->
            throwNoSuchRow(Value, TTName);
          _Else ->
            dict:store({TTName, FkName},
              {Parent, parents(Parent, RefTable, Tables, TxId)},
              Dict)
        end;
      _Else ->
        Dict
    end
  end, dict:new(), Fks).


get_by_name(ColName, [{{ColName, _Type}, Value} | _]) ->
  Value;
get_by_name(ColName, [_KV | Data]) ->
  get_by_name(ColName, Data);
get_by_name(_ColName, []) -> undefined.

get(ColName, Element) ->
  Columns = attributes(Element),
  Col = maps:get(ColName, Columns),
  AQL = column:type(Col),
  Constraint = column:constraint(Col),
  get(ColName, types:to_crdt(AQL, Constraint), Element).

get(ColName, Crdt, Element) when ?is_element(Element) ->
  get(ColName, Crdt, data(Element), table(Element)).

get(ColName, Crdt, Data, Table) when is_atom(Crdt) ->
  Value = proplists:get_value(?MAP_KEY(ColName, Crdt), Data),
  case Value of
    undefined ->
      TName = table:name(Table),
      throwNoSuchColumn(ColName, TName);
    _Else ->
      Value
  end;
get(ColName, Cols, Data, TName) ->
  Col = maps:get(ColName, Cols),
  AQL = column:type(Col),
  Constraint = column:constraint(Col),
  get(ColName, types:to_crdt(AQL, Constraint), Data, TName).

insert(Element) ->
  Ops = ops(Element),
  Key = primary_key(Element),
  crdt:map_update(Key, Ops).
insert(Element, TxId) ->
  Op = insert(Element),
  antidote_handler:update_objects(Op, TxId).

append(Key, Value, AQL, Constraint, Element) ->
  Data = data(Element),
  Ops = ops(Element),
  OffValue = apply_offset(Key, AQL, Constraint, Value),
  OpKey = ?MAP_KEY(Key, types:to_crdt(AQL, Constraint)),
  OpVal = types:to_insert_op(AQL, Constraint, OffValue),
  case OpVal of
    ?IGNORE_OP ->
      Element;
    _Else ->
      Element1 = set_data(Element, lists:append(Data, [{OpKey, Value}])),
      set_ops(Element1, utils:proplists_upsert(OpKey, OpVal, Ops))
  end.

apply_offset(Key, AQL, Constraint, Value) when is_atom(Key) ->
  case {AQL, Constraint} of
    {?AQL_COUNTER_INT, ?CHECK_KEY({Key, ?COMPARATOR_KEY(Comp), Offset})} ->
      bcounter:to_bcounter(Key, Value, Offset, Comp);
    _Else -> Value
  end;
apply_offset(_Key, _AQL, _Constraint, Value) -> Value.

foreign_keys(Fks, Element) when is_tuple(Element) ->
  Data = data(Element),
  TName = table(Element),
  foreign_keys(Fks, Data, TName).

foreign_keys(Fks, Data, TName) ->
  lists:map(fun(?T_FK(CName, CType, FkTable, FkAttr, DeleteRule)) ->
    Value = get(CName, types:to_crdt(CType, ?IGNORE_OP), Data, TName),
    {{CName, CType}, {FkTable, FkAttr}, DeleteRule, Value}
  end, Fks).

implicit_state(Table, RecordData, Tables, TxId) ->
  FKs = table:shadow_columns(Table),
  implicit_state(Table, RecordData, Tables, FKs, TxId).

implicit_state(Table, Data, Tables, [?T_FK(FKName, _, FKTName, _, _) | Fks], TxId)
  when length(FKName) == 1 ->
  Policy = table:policy(Table),

  FKTable = table:lookup(FKTName, Tables),
  IsVisible =
    case crp:dep_level(Policy) of
      ?REMOVE_WINS ->
        {_, RefVersion, RefData} = get_parent_info(Data, Table, FKName, FKTable, TxId),
        FkVersion = get(?VERSION, ?VERSION_TYPE, RefData, FKTable),
        FkVersion =:= RefVersion andalso
          is_visible(RefData, FKTName, Tables, TxId);
      _ ->
        case crp:dep_level(table:policy(FKTable)) of
          ?REMOVE_WINS ->
            {_, _, RefData} = get_parent_info(Data, Table, FKName, FKTable, TxId),
            is_visible(RefData, FKTName, Tables, TxId);
          _ ->
            true
        end
    end,

  IsVisible andalso implicit_state(Table, Data, Tables, Fks, TxId);
implicit_state(Table, Data, Tables, [_Fk | Fks], TxId) ->
  true andalso implicit_state(Table, Data, Tables, Fks, TxId);
implicit_state(_Table, _Data, _Tables, [], _TxId) ->
  true.

get_parent_info(Record, Table, FKName, FKTable, TxId) ->
  {RefValue, RefVersion} = element:get(FKName, ?CRDT_VARCHAR, Record, Table),
  FKData = read_record(RefValue, FKTable, TxId),
  case FKData of
    [] ->
      throwNoSuchRow(RefValue, table:name(FKTable));
    _ ->
      {RefValue, RefVersion, FKData}
  end.

delete(ObjKey, TxId) ->
  StateOp = crdt:field_map_op(element:st_key(), crdt:assign_lww(ipa:delete())),
  Update = crdt:map_update(ObjKey, StateOp),
  ok = antidote_handler:update_objects(Update, TxId),
  false.

read_record(Key, Table, TxId) ->
  BoundKey = create_key_from_table(Key, Table, TxId),
  {ok, [Object]} = antidote_handler:read_objects(BoundKey, TxId),
  Object.

shared_read(Key, Table, TxId) ->
  BoundKey = create_key_from_table(Key, Table, TxId),
  TPolicy = table:policy(Table),
  case crp:p_dep_level(TPolicy) of
    ?NO_CONCURRENCY ->
      lock_manager:acquire_shared_lock(BoundKey, TxId);
    _ ->
      ok
  end,
  {ok, [Object]} = antidote_handler:read_objects(BoundKey, TxId),
  Object.

exclusive_read(Key, Table, TxId) ->
  BoundKey = create_key_from_table(Key, Table, TxId),
  TPolicy = table:policy(Table),
  case crp:p_dep_level(TPolicy) of
    ?NO_CONCURRENCY ->
      lock_manager:acquire_exclusive_lock(BoundKey, TxId);
    _ ->
      ok
  end,
  {ok, [Object]} = antidote_handler:read_objects(BoundKey, TxId),
  Object.

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

primary_key_test() ->
  Table = eutils:create_table_aux(),
  Element = new(key, Table),
  ?assertEqual(create_key(key, 'Universities'), primary_key(Element)).

attributes_test() ->
  Table = eutils:create_table_aux(),
  Columns = table:columns(Table),
  Element = new(key, Table),
  ?assertEqual(Columns, attributes(Element)).

create_key_test() ->
  Key = key,
  TName = test,
  Expected = crdt:create_bound_object(Key, ?CRDT_TYPE, TName),
  ?assertEqual(Expected, create_key(Key, TName)).

new_test() ->
  Key = key,
  Table = eutils:create_table_aux(),
  BoundObject = create_key(Key, table:name(Table)),
  Ops = [crdt:field_map_op(st_key(), crdt:assign_lww(ipa:new()))],
  Expected = ?T_ELEMENT(BoundObject, Table, Ops, []),
  Expected1 = load_defaults(Expected),
  Element = new(Key, Table),
  ?assertEqual(Expected1, Element),
  ?assertEqual(crdt:assign_lww(ipa:new()), proplists:get_value(st_key(), ops(Element))).

new_1_test() ->
  Table = eutils:create_table_aux(),
  ?assertEqual(new(?EL_ANON, Table), new(Table)).

append_raw_test() ->
  Table = eutils:create_table_aux(),
  Value = 9,
  Element = new(key, Table),
  % assert not fail
  append('NationalRank', Value, ?AQL_INTEGER, ?IGNORE_OP, Element).

get_default_test() ->
  Table = eutils:create_table_aux(),
  El = new(key, Table),
  ?assertEqual("aaa", get('InstitutionId', ?CRDT_VARCHAR, El)).

get_by_name_test() ->
  Data = [{{a, abc}, 1}, {{b, abc}, 2}],
  ?assertEqual(1, get_by_name(a, Data)),
  ?assertEqual(undefined, get_by_name(c, Data)),
  ?assertEqual(undefined, get_by_name(a, [])).

-endif.

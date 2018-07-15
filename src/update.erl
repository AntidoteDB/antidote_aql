
-module(update).

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([exec/3]).

-export([table/1,
        set/1,
        where/1]).

%%====================================================================
%% API
%%====================================================================

exec({Table, Tables}, Props, TxId) ->
  TName = table:name(Table),
  SetClause = set(Props),
  WhereClause = where(Props),

  RawKeys = where:raw_keys(TName, WhereClause, TxId),
  VisibleKeys = lists:foldl(fun(Key, AccKeys) ->
    IndexEntry = index:p_keys(TName, {get, Key}, TxId),
    case element:is_visible(IndexEntry, TName, Tables, TxId) of
      true -> AccKeys ++ [IndexEntry]; %[element:create_key(Key, TName)];
      false -> AccKeys
    end
  end, [], RawKeys),

  StateOp = crdt:field_map_op(element:st_key(), crdt:assign_lww(ipa:insert())),

  MapUpdates =
    lists:map(fun(Entry) ->
      Key = index:entry_bobj(Entry),
      FieldUpdates = create_update(Table, Tables, [], SetClause, TxId),
      crdt:map_update(Key, lists:append([StateOp], lists:flatten(FieldUpdates)))
    end, VisibleKeys),

  UpdateMsg =
    case MapUpdates of
      [] -> ok;
      _Else ->
        antidote:update_objects(MapUpdates, TxId)
    end,

  case UpdateMsg of
    ok ->
      lists:foreach(fun(Entry) -> touch_cascade(Entry, Table, Tables, TxId) end, VisibleKeys),
      ok;
    Msg -> Msg
  end.

table({TName, _Set, _Where}) -> TName.

set({_TName, ?SET_CLAUSE(Set), _Where}) -> Set.

where({_TName, _Set, Where}) -> Where.

%%====================================================================
%% Internal functions
%%====================================================================

create_update(Table, Tables, Acc, [{ColumnName, Op, OpParam} | Tail], TxId) ->
  TName = table:name(Table),
  Column = column:s_get(Table, ColumnName),
  {ok, Updates} = resolve_op(Column, Op, OpParam, TName, Tables, TxId),
  case Updates of
    [?IGNORE_OP] ->
      create_update(Table, Tables, Acc, Tail, TxId);
    _Else ->
      create_update(Table, Tables, [Updates | Acc], Tail, TxId)
  end;
create_update(_Table, _Tables, Acc, [], _TxId) ->
  Acc.

% varchar -> assign
resolve_op(Column, ?ASSIGN_OP(_TChars), Value, TName, Tables, TxId) when is_list(Value) ->
  Op = fun crdt:assign_lww/1,
  resolve_op(Column, ?AQL_VARCHAR, Op, Value, TName, Tables, TxId);
% integer -> assign
resolve_op(Column, ?ASSIGN_OP(_TChars), Value, TName, Tables, TxId) ->
  Op = fun crdt:set_integer/1,
  resolve_op(Column, ?AQL_INTEGER, Op, Value, TName, Tables, TxId);
% counter -> increment
resolve_op(Column, ?INCREMENT_OP(_TChars), Value, TName, Tables, TxId) ->
  {IncFun, DecFun} = counter_functions(increment, Column),
  Op = resolve_op_counter(Column, IncFun, DecFun),
  resolve_op(Column, ?AQL_COUNTER_INT, Op, Value, TName, Tables, TxId);
% counter -> decrement
resolve_op(Column, ?DECREMENT_OP(_Tchars), Value, TName, Tables, TxId) ->
  {DecFun, IncFun} = counter_functions(decrement, Column),
  Op = resolve_op_counter(Column, DecFun, IncFun),
  resolve_op(Column, ?AQL_COUNTER_INT, Op, Value, TName, Tables, TxId).

resolve_op(Column, AQL, Op, Value, TName, Tables, TxId) ->
  CName = column:name(Column),
  CType = column:type(Column),
  Constraint = column:constraint(Column),
  case CType of
    AQL ->
      FKUpdate = resolve_foreign_key({CName, CType, Constraint}, Value, TName, Tables, TxId),
      Update = crdt:field_map_op(CName, types:to_crdt(AQL, Constraint), Op(Value)),
      {ok, lists:flatten([Update], FKUpdate)};
    _Else ->
      resolve_fail(CName, CType)
  end.

resolve_op_counter(Column, Forward, Reverse) ->
  case column:constraint(Column) of
    ?CHECK_KEY({_Key, ?COMPARATOR_KEY(Comp), _Offset}) ->
      case Comp of
        ?PARSER_GREATER ->
          Forward;
        _Else ->
          Reverse
      end;
    _Else ->
      Forward
  end.

resolve_foreign_key({CName, CType, ?FOREIGN_KEY(_) = FK}, FkValue, TName, Tables, TxId) ->
  ?FOREIGN_KEY({FkTName, _, _}) = FK,
  IndexEntry = index:p_keys(FkTName, {get, FkValue}, TxId),
  case element:is_visible(IndexEntry, FkTName, Tables, TxId) of
    false ->
      ErrorMsg = io_lib:format("Cannot find row ~p in table ~p", [utils:to_atom(FkValue), FkTName]),
      throw(lists:flatten(ErrorMsg));
    _Else ->
      %Table = table:lookup(TName, Tables),
      %FKs = table:shadow_columns(Table),
      %?T_FK(RName, _, _, _, _) = lists:keyfind({[{TName, CName}], CType}, 1, FKs),

      ParentVersion = index:entry_version(IndexEntry),
      SendValue = {FkValue, ParentVersion},

      OpKey = ?MAP_KEY([{TName, CName}], types:to_crdt(CType, ?IGNORE_OP)),
      OpVal = types:to_insert_op(CType, ?IGNORE_OP, SendValue),

      [{OpKey, OpVal}]
  end;
resolve_foreign_key(_, _, _, _, _) ->
  [].

counter_functions(increment, ?T_COL(_, _, ?CHECK_KEY(_))) ->
  {fun crdt:increment_bcounter/1, fun crdt:decrement_bcounter/1};
counter_functions(decrement, ?T_COL(_, _, ?CHECK_KEY(_))) ->
  {fun crdt:decrement_bcounter/1, fun crdt:increment_bcounter/1};
counter_functions(increment, _) ->
  {fun crdt:increment_counter/1, fun crdt:decrement_counter/1};
counter_functions(decrement, _) ->
  {fun crdt:decrement_counter/1, fun crdt:increment_counter/1}.

resolve_fail(CName, CType) ->
  Msg = io_lib:format("Cannot assign to column ~p of type ~p", [CName, CType]),
  {err, lists:flatten(Msg)}.

touch_cascade(Entry, Table, Tables, TxId) ->
  insert:touch_cascade(entry, Entry, Table, Tables, TxId).

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).
create_column_aux(CName, CType) ->
  ?T_COL(CName, CType, ?NO_CONSTRAINT).

resolve_op_varchar_test() ->
  CName = col1,
  CType = ?AQL_VARCHAR,
  Column = create_column_aux(CName, CType),
  Value = "Value",
  Expected = {ok, [crdt:field_map_op(CName, ?CRDT_VARCHAR, crdt:assign_lww(Value))]},
  Actual = resolve_op(Column, ?ASSIGN_OP("SomeChars"), Value, table, [], ?IGNORE_OP),
  ?assertEqual(Expected, Actual).

resolve_op_integer_test() ->
  CName = col1,
  CType = ?AQL_INTEGER,
  Column = create_column_aux(CName, CType),
  Value = 2,
  Expected = {ok, [crdt:field_map_op(CName, ?CRDT_INTEGER, crdt:set_integer(Value))]},
  Actual = resolve_op(Column, ?ASSIGN_OP(2), Value, table, [], ?IGNORE_OP),
  ?assertEqual(Expected, Actual).

resolve_op_counter_increment_test() ->
  CName = col1,
  CType = ?AQL_COUNTER_INT,
  Column = create_column_aux(CName, CType),
  Value = 2,
  Expected = {ok, [crdt:field_map_op(CName, ?CRDT_COUNTER_INT, crdt:increment_counter(Value))]},
  Actual = resolve_op(Column, ?INCREMENT_OP(3), Value, table, [], ?IGNORE_OP),
  ?assertEqual(Expected, Actual).

resolve_op_counter_decrement_test() ->
  CName = col1,
  CType = ?AQL_COUNTER_INT,
  Column = create_column_aux(CName, CType),
  Value = 2,
  Expected = {ok, [crdt:field_map_op(CName, ?CRDT_COUNTER_INT, crdt:decrement_counter(Value))]},
  Actual = resolve_op(Column, ?DECREMENT_OP(3), Value, table, [], ?IGNORE_OP),
  ?assertEqual(Expected, Actual).

-endif.

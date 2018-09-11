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

exec({Table, Tables}, Props, TxId) ->
  TName = table(Props),
  Condition = where(Props),
  Keys = where:scan(TName, Condition, TxId),
  lists:foreach(fun(Key) ->
    case delete_cascade(Key, Table, Tables, TxId) of
      [] -> ok;
      DeleteUpds ->
        ok = antidote_handler:update_objects(DeleteUpds, TxId)
    end
  end, Keys).

table({TName, _Where}) -> TName.

where({_TName, Where}) -> Where.

%% ====================================================================
%% Internal functions
%% ====================================================================

delete_cascade(Key, Table, Tables, TxId) ->
  Data = element:exclusive_read(Key, Table, TxId),
  %{ok, [Data]} = antidote_handler:read_objects(Key, TxId),
  case length(Data) of
    0 -> [];
    1 -> [];
    _Else ->
      [?T_COL(PkName, PkAQLType, PkConst)] = column:s_primary_key(Table),
      RawKey = element:get(PkName, types:to_crdt(PkAQLType, PkConst), Data, Table),
      delete_cascade_dependants({Key, RawKey}, Table, Tables, TxId)
  end.

delete_cascade_dependants({Key, RawKey}, Table, Tables, TxId) ->
  Dependants = cascade_dependants({Key, RawKey}, Table, Tables, TxId),
  DeleteUpdates = lists:foldl(fun({TName, Keys}, AccUpds) ->
    DepTable = table:lookup(TName, Tables),
    lists:foldl(fun(K, AccUpds2) ->
      case delete_cascade(K, DepTable, Tables, TxId) of
        [] ->
          AccUpds2;
        DeleteUpds ->
          lists:append([AccUpds2, DeleteUpds])
      end
    end, AccUpds, Keys)
  end, [], Dependants),
  lists:append([crdt:ipa_update(Key, ipa:delete())], DeleteUpdates).

cascade_dependants(Key, Table, Tables, TxId) ->
  cascade_dependants(Key, Table, Tables, Tables, TxId, []).

cascade_dependants(Key, Table, AllTables, [{_TName, Table} | Tables], TxId, Acc) ->
  cascade_dependants(Key, Table, AllTables, Tables, TxId, Acc);
cascade_dependants({Key, RawKey}, Table, AllTables, [{{T1TName, _}, Table2} | Tables], TxId, Acc) ->
  TName = table:name(Table),
  Fks = table:shadow_columns(Table2),
  Refs = fetch_cascade({Key, RawKey}, TName, T1TName, AllTables, Fks, TxId, []),
  case Refs of
    error ->
      {PKey, _, _} = Key,
      ErrorMsg = ?REFINTEG_ERROR(TName, PKey),
      throw(lists:flatten(ErrorMsg));
    [] ->
      cascade_dependants({Key, RawKey}, Table, AllTables, Tables, TxId, Acc);
    _Else ->
      cascade_dependants({Key, RawKey}, Table, AllTables, Tables, TxId, lists:append(Acc, [{T1TName, Refs}]))
  end;
cascade_dependants(_Key, _Table, _AccTables, [], _TxId, Acc) -> Acc.

fetch_cascade({Key, RawKey}, TName, TDepName, Tables,
  [?T_FK(Name, _Type, TName, _Attr, ?CASCADE_TOKEN) | Fks], TxId, Acc)
  when length(Name) == 1 ->
  [{TDepName, DepAttr}] = Name,
  DepTable = table:lookup(TDepName, Tables),
  [?T_COL(DepPkName, _, _)] = column:s_primary_key(DepTable),
  DepFilter = {TDepName, [DepPkName], [{DepAttr, ?PARSER_EQUALITY, RawKey}]},
  {ok, DependantRows} = select:exec({DepTable, ignore}, DepFilter, TxId),

  case DependantRows of
    [] ->
      fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, Acc);
    _Else ->
      DependantKeys =
        lists:map(fun([{_, DepRawValue}]) ->
          element:create_key_from_table(DepRawValue, DepTable, TxId)
        end, DependantRows),
      fetch_cascade(Key, TName, TDepName, Tables, Fks, TxId, lists:append(Acc, DependantKeys))
  end;
fetch_cascade({Key, RawKey}, TName, TDepName, Tables,
  [?T_FK(Name, _Type, TName, _Attr, ?RESTRICT_TOKEN) | FKs], TxId, Acc)
  when length(Name) == 1 ->
  [{TDepName, DepAttr}] = Name,
  DepTable = table:lookup(TDepName, Tables),
  [?T_COL(DepPkName, _, _)] = column:s_primary_key(DepTable),
  DepFilter = {TDepName, [DepPkName], [{DepAttr, ?PARSER_EQUALITY, RawKey}]},
  {ok, DependantRows} = select:exec({DepTable, ignore}, DepFilter, TxId),

  case DependantRows of
    [] ->
      fetch_cascade(Key, TName, TDepName, Tables, FKs, TxId, Acc);
    _Else ->
      error
  end;

fetch_cascade(Key, TName, TDepName, Tables, [_Fk | FKs], TxId, Acc) ->
  fetch_cascade(Key, TName, TDepName, Tables, FKs, TxId, Acc);
fetch_cascade(_Key, _TName, _TDepName, _Tables, [], _TxId, Acc) ->
  Acc.
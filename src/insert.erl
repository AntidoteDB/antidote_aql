%% @author Joao
%% @doc @todo Add description to insert.


-module(insert).

-define(NO_PK, none).

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([exec/3]).

-export([table/1,
  keys/2,
  values/1]).

-export([touch_cascade/4]).

exec({Table, Tables}, Props, TxId) ->
  Keys = keys(Props, Table),
  Values = values(Props),
  Keys1 = handle_defaults(Keys, Values, Table),
  AnnElement = element:new(Table),
  {ok, Element} = element:put(Keys1, Values, AnnElement),
  Element1 = element:set_version(Element, TxId),
  Parents = element:parents(element:data(Element1), Table, Tables, TxId),

  Element2 = element:build_fks(Element1, Parents),
  ok = element:insert(Element2, TxId),

  touch_cascade(Parents, Table, Tables, TxId),
  %touch_cascade(element, Element2, Table, Tables, TxId),
  ok.

table({TName, _Keys, _Values}) -> TName.

keys({_TName, Keys, _Values}, Table) ->
  case Keys of
    ?PARSER_WILDCARD ->
      column:s_names(Table);
    _Else ->
      Keys
  end.

values({_TName, _Keys, Values}) -> Values.

%touch_cascade(record, Record, Table, Tables, TxId) ->
%  TName = table:name(Table),
%  Fks = element:foreign_keys(foreign_keys:from_table(Table), Record, TName),
%  FksKV = read_fks(Fks, Tables, TxId, true),
%  lists:foreach(fun({Fk, Data}) -> touch(Fk, Data, Tables, TxId) end, FksKV);
%touch_cascade(element, Element, Table, Tables, TxId) ->
%  Fks = element:foreign_keys(foreign_keys:from_table(Table), Element),
%  FksKV = read_fks(Fks, Tables, TxId, true),
%  lists:foreach(fun({Fk, Data}) -> touch(Fk, Data, Tables, TxId) end, FksKV).

touch_cascade(Parents, Table, Tables, TxId) ->
  Fks = table:shadow_columns(Table),
  lists:foreach(fun(Fk) -> touch(Fk, Parents, Tables, TxId) end, Fks).
%touch_cascade(element, Element, Table, Tables, TxId) ->
%  Fks = element:foreign_keys(foreign_keys:from_table(Table), Element),
%  FksKV = read_fks(Fks, Tables, TxId, true),
%  lists:foreach(fun({Fk, Data}) -> touch(Fk, Data, Tables, TxId) end, FksKV).

%% ====================================================================
%% Functions for inserts and updates
%% ====================================================================

%% Data = element:data(Element),
%%
%%fetch_hierarchy(Data, Table, Tables, TxId) ->
%%  Fks = table:shadow_columns(Table),
%%  lists:foldl(fun(?T_FK(Name, Type, TTName, _, _), Dict) ->
%%    case Name of
%%      [ShCol] ->
%%        {_FkTable, FkName} = ShCol,
%%        Value = element:get(FkName, types:to_crdt(Type, ?IGNORE_OP), Data, Table),
%%        RefTable = table:lookup(TTName, Tables),
%%        Parent = element:shared_read(Value, RefTable, TxId),
%%        case Parent of
%%          [] ->
%%            element:throwNoSuchRow(Value, TTName);
%%          _Else ->
%%            dict:store({Value, TTName}, {Parent, fetch_hierarchy(Parent, TTName, Tables, TxId)}, Dict)
%%        end;
%%      _Else -> Dict
%%    end
%%  end, dict:new(), Fks).

%%fetch_hierarchy(element, Element, Table, Tables, TxId) ->
%%Fks = element:foreign_keys(foreign_keys:from_table(Table), Element),
%%FksKV = read_fks(Fks, Tables, TxId, true),
%%{element:, lists:foreach(fun({Value, PTabName, Data}) -> fetch_hierarchy0(Fk, Data, Tables, TxId) end, FksKV).
%%
%%fetch_hierarchy0({_Col, {PTabName, _PTabAttr}, _DelRule, Value}, Data, Tables, TxId) ->
%%  Table = table:lookup(PTabName, Tables),
%%  TKey = element:create_key_from_table(Value, Table, TxId),
%%  Policy = table:policy(Table),
%%
%%  % touch parents
%%  Fks = element:foreign_keys(foreign_keys:from_table(Table), Data, PTabName),
%%  FksKV = read_fks(Fks, Tables, TxId, false),
%%  lists:foreach(fun({Fk, Data2}) -> touch(Fk, Data2, Tables, TxId) end, FksKV).

%%read_fks(Fks, Tables, TxId, false) ->
%%  lists:map(fun({_Col, {PTabName, _PTabAttr}, _DelRule, Value} = Fk) ->
%%    PTable = table:lookup(PTabName, Tables),
%%    Data = element:shared_read(Value, PTable, TxId),
%%    {Fk, Data}
%%  end, Fks);
%%read_fks(Fks, Tables, TxId, true) ->
%%  lists:map(fun({_Col, {PTabName, _PTabAttr}, _DelRule, Value} = Fk) ->
%%    PTable = table:lookup(PTabName, Tables),
%%    Data = element:shared_read(Value, PTable, TxId),
%%    case element:is_visible(Data, PTabName, Tables, TxId) of
%%      false ->
%%        element:throwNoSuchRow(Value, PTabName);
%%      _Else ->
%%        {Fk, Data}
%%    end
%%  end, Fks).

%%touch({_Col, {PTabName, _PTabAttr}, _DelRule, Value}, Data, Tables, TxId) ->
%%  Table = table:lookup(PTabName, Tables),
%%  TKey = element:create_key_from_table(Value, Table, TxId),
%%  Policy = table:policy(Table),
%%  case crp:p_dep_level(Policy) of
%%    ?REMOVE_WINS -> ok;
%%    ?NO_CONCURRENCY -> ok;
%%    _Else -> antidote_handler:update_objects(crdt:ipa_update(TKey, ipa:touch()), TxId)
%%  end,
%%
%%  % touch parents
%%  Fks = element:foreign_keys(foreign_keys:from_table(Table), Data, PTabName),
%%  FksKV = read_fks(Fks, Tables, TxId, false),
%%  lists:foreach(fun({Fk, Data2}) -> touch(Fk, Data2, Tables, TxId) end, FksKV).

touch(?T_FK(FkName, _, FkTabName, FkColName, _), Parents, Tables, TxId)
  when length(FkName) == 1 ->
  PTable = table:lookup(FkTabName, Tables),

  [{_, ParentId}] = FkName,
  {Parent, PParents} = dict:fetch({FkTabName, ParentId}, Parents),
  Value = element:get_by_name(foreign_keys:to_cname(FkColName), Parent),

  %TName = table:name(Table),
  %PTable = table:lookup(PTabName, Tables),
  %Value = element:get_by_name(foreign_keys:to_cname({TName, Col}), Parent),
  TKey = element:create_key_from_table(Value, PTable, TxId),
  Policy = table:policy(PTable),
  case crp:p_dep_level(Policy) of
    ?ADD_WINS -> antidote_handler:update_objects(crdt:ipa_update(TKey, ipa:touch()), TxId);
    _Else -> ok
  end,

  % touch parents
  Fks = table:shadow_columns(PTable),
  lists:foreach(fun(Fk) -> touch(Fk, PParents, Tables, TxId) end, Fks);
touch(_, _, _, _) ->
  ok.

%% ====================================================================
%% Internal functions
%% ====================================================================

handle_defaults(Keys, Values, Table) ->
  if
    length(Keys) =:= length(Values) ->
      Keys;
    true ->
      Defaults = column:s_filter_defaults(Table),
      lists:filter(fun(Key) ->
        not maps:is_key(Key, Defaults)
  end, Keys)
  end.
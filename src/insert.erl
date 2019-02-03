%%%-------------------------------------------------------------------
%%% @author João Sousa, Pedro Lopes
%%% @doc A module to handle AQL insert operations.
%%% @end
%%%-------------------------------------------------------------------

-module(insert).

-define(NO_PK, none).

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
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

touch_cascade(Parents, Table, Tables, TxId) ->
  Fks = table:shadow_columns(Table),
  lists:foreach(fun(Fk) -> touch(Fk, Parents, Tables, TxId) end, Fks).

%% ===================================================================
%% Functions for inserts and updates
%% ===================================================================

touch(?T_FK(FkName, _, FkTabName, FkColName, _), Parents, Tables, TxId)
  when length(FkName) == 1 ->
  PTable = table:lookup(FkTabName, Tables),

  [{_, ParentId}] = FkName,
  {Parent, PParents} = dict:fetch({FkTabName, ParentId}, Parents),
  Value = element:get_by_name(foreign_keys:to_cname(FkColName), Parent),

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

%% ===================================================================
%% Internal functions
%% ===================================================================

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
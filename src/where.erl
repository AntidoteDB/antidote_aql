%%%-------------------------------------------------------------------
%%% @author João Sousa, Pedro Lopes
%%% @doc A module to handle AQL where clauses.
%%% @end
%%%-------------------------------------------------------------------

-module(where).

-include("parser.hrl").
-include("aql.hrl").

-export([scan/3]).

scan(Table, ?PARSER_WILDCARD, TxId) ->
  TName = table:name(Table),
  Index = index:p_keys(TName, TxId),
  lists:map(fun({_Key, BObj}) -> BObj end, Index);
scan(Table, Conditions, TxId) ->
  evaluate(Table, Conditions, TxId, []).

%% ====================================================================
%% Internal functions
%% ====================================================================

evaluate(Table, [{_ClValue, Arop, Value} | T], TxId, Acc) ->
  case Arop of
    ?PARSER_EQUALITY ->
      NewAcc = lists:flatten(Acc, [element:create_key_from_table(Value, Table, TxId)]),
      evaluate(Table, T, TxId, NewAcc);
    _Else ->
      throw("Not supported yet")
  end;
evaluate(_Table, [], _TxId, Acc) ->
  Acc.

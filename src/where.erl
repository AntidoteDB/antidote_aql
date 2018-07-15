
-module(where).

-include("parser.hrl").
-include("aql.hrl").

-export([scan/3, raw_keys/3]).

scan(TName, ?PARSER_WILDCARD, TxId) ->
  index:p_keys(TName, TxId);
scan(TName, Conditions, TxId) ->
  Keys = evaluate(TName, Conditions, []),
  filter_keys(Keys, TName, TxId).

raw_keys(TName, ?PARSER_WILDCARD, TxId) ->
  PIndex = index:primary_index(TName, TxId),
  case PIndex of
    {_IdxPol, _DepPol, IndexTree} ->
      lists:map(fun({Key, _Entry}) -> Key end, IndexTree);
    [] -> []
  end;
raw_keys(_TName, Conditions, _TxId) ->
  raw(Conditions, []).

%% ====================================================================
%% Internal functions
%% ====================================================================

evaluate(TName, [{_ClValue, Arop, Value} | T], Acc) ->
  case Arop of
    ?PARSER_EQUALITY ->
      NewAcc = lists:flatten(Acc, [element:create_key(Value, TName)]),
      evaluate(TName, T, NewAcc);
    _Else ->
      throw("Not supported yet! :)")
  end;
evaluate(_TName, [], Acc) ->
  Acc.

raw([{_ClValue, Arop, Value} | T], Acc) ->
  case Arop of
    ?PARSER_EQUALITY ->
      NewAcc = lists:append(Acc, [Value]),
      raw(T, NewAcc);
    _Else ->
      throw("Not supported yet! :)")
  end;
raw([], Acc) ->
  Acc.

filter_keys(Keys, TName, TxId) ->
  Index = index:p_keys(TName, TxId),
  lists:foldl(fun({K, _T, _B} = Key, AccKeys) ->
      case lists:keyfind(K, 1, Index) of
          false ->
              io:format("Error: Cannot update row with value ~p. Row does not exist.~n", [K]),
              AccKeys;
          _Else ->
              lists:append(AccKeys, [Key])
      end
  end, [], Keys).

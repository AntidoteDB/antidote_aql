-module(where).

-include("parser.hrl").
-include("aql.hrl").

-export([scan/3]).

scan(TName, ?PARSER_WILDCARD, TxId) ->
    Index = index:p_keys(TName, TxId),
    lists:map(fun({_Key, BObjList}) ->
        [BObj | _Rest] = BObjList,
        BObj
    end, Index);
scan(TName, Conditions, TxId) ->
    Keys = evaluate(TName, Conditions, []),
    filter_keys(Keys, TName, TxId).


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

filter_keys(Keys, TName, TxId) ->
    Index = index:p_keys(TName, TxId),
    lists:foldl(fun({K, _T, _B} = Key, AccKeys) ->
        case lists:keyfind(K, 1, Index) of
            false ->
                io:format("Error: Cannot update/delete row with value ~p. Row does not exist.~n", [K]),
                AccKeys;
            _Else ->
                lists:append(AccKeys, [Key])
        end
    end, [], Keys).

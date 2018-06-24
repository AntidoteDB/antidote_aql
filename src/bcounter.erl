
-module(bcounter).

-include("parser.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([to_bcounter/3,
        to_bcounter/4,
        from_bcounter/3,
        inv_from_bcounter/3,
        value/1]).

to_bcounter(Value, Offset, Comp) ->
  apply_offset_value(Comp, Offset, Value).
to_bcounter(Key, Value, Offset, Comp) ->
  OffValue = apply_offset_value(Comp, Offset, Value),
  check_bcounter_value(Comp, Key, OffValue).

check_bcounter_value(?PARSER_GEQ, _Key, Value) when Value >= 0 -> Value;
check_bcounter_value(?PARSER_LEQ, _Key, Value) when Value >= 0 -> Value;
check_bcounter_value(?PARSER_GREATER, _Key, Value) when Value >= 0 -> Value;
check_bcounter_value(?PARSER_LESSER, _Key, Value) when Value >= 0 -> Value;
check_bcounter_value(_, Key, Value) -> throw(lists:concat(["Invalid value ", Value, " for column ", Key])).

apply_offset_value(?PARSER_GREATER, Offset, Value) -> Value - Offset - 1;
apply_offset_value(?PARSER_GEQ, Offset, Value) -> Value - Offset;
apply_offset_value(?PARSER_LESSER, Offset, Value) -> -1 * (Value - Offset) - 1;
apply_offset_value(?PARSER_LEQ, Offset, Value) -> -1 * (Value - Offset).

from_bcounter(Comp, {_I, _D} = Value, Offset) ->
  BCValue = value(Value),
  from_bcounter(Comp, BCValue, Offset);
from_bcounter(?PARSER_GREATER, Value, Offset) -> Value + Offset + 1;
from_bcounter(?PARSER_GEQ, Value, Offset) -> Value + Offset;
from_bcounter(?PARSER_LESSER, Value, Offset) -> -1 * (Value + Offset * -1 + 1);
from_bcounter(?PARSER_LEQ, Value, Offset) -> -1 * (Value + Offset * -1).

inv_from_bcounter(Comp, {_I, _D} = Value, Offset) ->
  BCValue = value(Value),
  inv_from_bcounter(Comp, BCValue, Offset);
inv_from_bcounter(?PARSER_GREATER, Value, Offset) ->
  Value - Offset;
inv_from_bcounter(?PARSER_GEQ, Value, Offset) ->
  Value - Offset;
inv_from_bcounter(?PARSER_LESSER, Value, Offset) ->
  Offset - Value;
inv_from_bcounter(?PARSER_LEQ, Value, Offset) ->
  Offset - Value.

value({Incs, Decs}) ->
  IncsList = orddict:to_list(Incs),
  DecsList = orddict:to_list(Decs),
  SumIncs = lists:foldl(fun sum/2, 0, IncsList),
  SumDecs = lists:foldl(fun sum/2, 0, DecsList),
  SumIncs-SumDecs.

sum({_Ids, Value}, Acc) -> Value+Acc.

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

to_bcounter_test() ->
  % greater than
  ?assertEqual(5, to_bcounter(k, 5, 0, ?PARSER_GREATER)),
  ?assertEqual(3, to_bcounter(k, 5, 2, ?PARSER_GREATER)),
  ?assertEqual(1, to_bcounter(k, 31, 30, ?PARSER_GREATER)),
  ?assertThrow(_, to_bcounter(k, 5, 6, ?PARSER_GREATER)),
  ?assertThrow(_, to_bcounter(k, 5, 5, ?PARSER_GREATER)),
  % smaller than
  ?assertEqual(5, to_bcounter(k, 0, 5, ?PARSER_LESSER)),
  ?assertEqual(3, to_bcounter(k, 2, 5, ?PARSER_LESSER)),
  ?assertEqual(1, to_bcounter(k, 30, 31, ?PARSER_LESSER)),
  ?assertThrow(_, to_bcounter(k, 6, 5, ?PARSER_LESSER)),
  ?assertThrow(_, to_bcounter(k, 5, 5, ?PARSER_LESSER)).

from_bcounter_test() ->
  % greater than
  ?assertEqual(31, from_bcounter(?PARSER_GREATER, 1, 30)),
  % smaller than
  ?assertEqual(4, from_bcounter(?PARSER_LESSER, 1, 5)).

-endif.

%%%-------------------------------------------------------------------
%%% @author João Sousa, Pedro Lopes
%%% @doc Bounded counter CRDT specific module to calculate output and
%%%      input counter values.
%%% @end
%%%-------------------------------------------------------------------

-module(bcounter).

-include("parser.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([to_bcounter/3,
  to_bcounter/4,
  from_bcounter/3,
  value/1]).

to_bcounter(Value, Offset, Comp) ->
  apply_offset_value(Comp, Offset, Value).
to_bcounter(Key, Value, Offset, Comp) ->
  OffValue = apply_offset_value(Comp, Offset, Value),
  check_bcounter_value(Comp, Key, OffValue, Value).

check_bcounter_value(?PARSER_GEQ, _Key, OffValue, _) when OffValue >= 0 -> OffValue;
check_bcounter_value(?PARSER_LEQ, _Key, OffValue, _) when OffValue >= 0 -> OffValue;
check_bcounter_value(?PARSER_GREATER, _Key, OffValue, _) when OffValue >= 0 -> OffValue;
check_bcounter_value(?PARSER_LESSER, _Key, OffValue, _) when OffValue >= 0 -> OffValue;
check_bcounter_value(_, Key, _, Value) ->
  MsgFormat = io_lib:format("Invalid value ~p for column ~p", [Value, Key]),
  throw(lists:flatten(MsgFormat)).

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

value({Incs, Decs}) ->
  IncsList = orddict:to_list(Incs),
  DecsList = orddict:to_list(Decs),
  SumIncs = lists:foldl(fun sum/2, 0, IncsList),
  SumDecs = lists:foldl(fun sum/2, 0, DecsList),
  SumIncs - SumDecs.

sum({_Ids, Value}, Acc) -> Value + Acc.

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

to_bcounter_test() ->
  % greater than
  ?assertEqual(4, to_bcounter(k, 5, 0, ?PARSER_GREATER)),
  ?assertEqual(2, to_bcounter(k, 5, 2, ?PARSER_GREATER)),
  ?assertEqual(0, to_bcounter(k, 31, 30, ?PARSER_GREATER)),
  ?assertThrow(_, to_bcounter(k, 5, 6, ?PARSER_GREATER)),
  ?assertThrow(_, to_bcounter(k, 5, 7, ?PARSER_GREATER)),
  % greater than or equal
  ?assertEqual(5, to_bcounter(k, 5, 0, ?PARSER_GEQ)),
  ?assertEqual(3, to_bcounter(k, 5, 2, ?PARSER_GEQ)),
  ?assertEqual(1, to_bcounter(k, 31, 30, ?PARSER_GEQ)),
  ?assertThrow(_, to_bcounter(k, 5, 6, ?PARSER_GEQ)),
  ?assertThrow(_, to_bcounter(k, 5, 7, ?PARSER_GEQ)),
  % lesser than
  ?assertEqual(4, to_bcounter(k, 0, 5, ?PARSER_LESSER)),
  ?assertEqual(2, to_bcounter(k, 2, 5, ?PARSER_LESSER)),
  ?assertEqual(0, to_bcounter(k, 30, 31, ?PARSER_LESSER)),
  ?assertThrow(_, to_bcounter(k, 5, 5, ?PARSER_LESSER)),
  ?assertThrow(_, to_bcounter(k, 6, 5, ?PARSER_LESSER)),
  % lesser than or equal
  ?assertEqual(5, to_bcounter(k, 0, 5, ?PARSER_LEQ)),
  ?assertEqual(3, to_bcounter(k, 2, 5, ?PARSER_LEQ)),
  ?assertEqual(1, to_bcounter(k, 30, 31, ?PARSER_LEQ)),
  ?assertThrow(_, to_bcounter(k, 6, 5, ?PARSER_LEQ)),
  ?assertThrow(_, to_bcounter(k, 7, 5, ?PARSER_LEQ)).

from_bcounter_test() ->
  % greater than
  ?assertEqual(32, from_bcounter(?PARSER_GREATER, 1, 30)),
  % greater than or equal
  ?assertEqual(31, from_bcounter(?PARSER_GEQ, 1, 30)),
  % smaller than
  ?assertEqual(3, from_bcounter(?PARSER_LESSER, 1, 5)),
  % lesser than or equal
  ?assertEqual(4, from_bcounter(?PARSER_LEQ, 1, 5)).

-endif.

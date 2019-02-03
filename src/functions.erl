%%%-------------------------------------------------------------------
%%% @author Pedro Lopes
%%% @doc An Erlang module for the (future) use of built-in functions
%%%      in queries.
%%% @end
%%%-------------------------------------------------------------------
-module(functions).

%% API
-export([parse_function/1]).

parse_function(Function) when is_atom(Function) ->
  FuncString = atom_to_list(Function),
  parse_function(FuncString);
parse_function(Function) when is_list(Function) ->
  ErrorMsg = lists:flatten(io_lib:format("Malformed function header: ~p", [Function])),
  try
    FParPos = string:str(Function, "("),
    LParPos = string:rstr(Function, ")"),
    FuncName = list_to_atom(string:sub_string(Function, 1, FParPos - 1)),
    Args = string:tokens(string:sub_string(Function, FParPos + 1, LParPos - 1), " ,"),
    {FuncName, Args}
  of
    {F, P} -> {F, P}
  catch
    _ -> throw(ErrorMsg)
  end.
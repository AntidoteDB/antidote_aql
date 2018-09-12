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

exec({Table, _Tables}, Props, TxId) ->
	Keys = keys(Props, Table),
	Values = values(Props),
	Keys1 = handle_defaults(Keys, Values, Table),
	AnnElement = element:new(Table),
	{ok, Element} = element:put(Keys1, Values, AnnElement),

	ok = element:insert(Element, TxId),
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
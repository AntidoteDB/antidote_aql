%%%-------------------------------------------------------------------
%%% @author Gonçalo Tomás, Pedro Lopes
%%% @doc A HTTP server to handle AQL queries from remote clients.
%%% @end
%%%-------------------------------------------------------------------

-module(aql_http_handler).
-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").
-behaviour(elli_handler).

handle(Req, _Args) ->
  %% Delegate to our handler function
  handle(Req#req.method, elli_request:path(Req), Req).

handle('POST', [<<"aql">>], Req) ->
  case elli_request:post_arg_decoded(<<"query">>, Req, <<"undefined">>) of
    <<"undefined">> ->
      {400, [], <<"No query parameter in POST request body!">>};
    Query ->
      io:format("Received query: ~p~n", [Query]),
      Result = aql:query(binary_to_list(Query)),
      case Result of
        {ok, QueryRes} ->
          Encoded = jsx:encode(QueryRes),
          io:format("Response: ~p~n", [Encoded]),
          {ok, [], Encoded};
        {ok, QueryRes, _Tx} ->
          Encoded = jsx:encode(QueryRes),
          io:format("Response: ~p~n", [Encoded]),
          {ok, [], Encoded};
        {error, Message, _} ->
          ErrorMsg = lists:concat(["Error: ", Message]),
          {500, [], list_to_binary(ErrorMsg)}
      end
  end;

handle(_, _, _Req) ->
  {404, [], <<"Not Found">>}.

%% @doc Handle request events, like request completed, exception
%% thrown, client timeout, etc. Must return `ok'.
handle_event(_Event, _Data, _Args) ->
  ok.
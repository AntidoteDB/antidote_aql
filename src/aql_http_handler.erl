-module(aql_http_handler).
-export([handle/2, handle_event/3]).

-include_lib("elli/include/elli.hrl").
-behaviour(elli_handler).

handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).

handle('POST',[<<"aql">>], Req) ->
    case elli_request:post_arg_decoded(<<"query">>, Req, <<"undefined">>) of
        <<"undefined">> ->
            {400, [], <<"No query parameter in POST request body!">>};
        Query ->
            io:format("Received query: ~p~n" , [Query]),
            {ok, [], jsx:encode(aqlparser:parse({str, binary_to_list(Query)}, 'antidote@127.0.0.1'))}
    end;

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.

%% @doc Handle request events, like request completed, exception
%% thrown, client timeout, etc. Must return `ok'.
handle_event(_Event, _Data, _Args) ->
    ok.

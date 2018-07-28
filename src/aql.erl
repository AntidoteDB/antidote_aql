%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc The AQL interface, responsible for the AQL application.
%%%      The AQL application is composed on an HTTP server, responsible
%%%      for receiving queries via an HTTP endpoint.
%%%
%%%      Interface:
%%%      - start():
%%%           Starts the AQL application with the default
%%%           configuration;
%%%      - stop():
%%%           Stops the AQL application;
%%%      - start_shell():
%%%           Starts the AQL shell with default configuration;
%%%      - start_shell(node):
%%%           Starts the AQL shell configured with the name of an
%%%           Antidote node;
%%%      - query(query, node):
%%%           Issues a query to an Antidote node.
%%%           The query is performed on a single transaction,
%%%           and committed or aborted after realization;
%%%      - query(query, node, transaction):
%%%           Issues a query to an Antidote node, on a given
%%%           transaction.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(aql).

%% Types
-type antidote_node() :: atom().
-type query() :: list().

%% API
-export([start/0, stop/0]).

-export([start_shell/0, start_shell/1]).
-export([query/2, query/3]).

-spec start() -> ok.
start() ->
  {ok, _} = application:ensure_all_started(aql).

-spec stop() -> ok.
stop() ->
  application:stop(aql).

-spec start_shell() -> term().
start_shell() ->
  start(),
  aqlparser:start_shell().

-spec start_shell(antidote_node()) -> term().
start_shell(AntidoteNode) ->
  start(),
  aqlparser:start_shell(AntidoteNode).

-spec query(query(), term()) -> {ok, term(), term()} | {ok, term()}.
query(Query, Node) ->
  aqlparser:parse({str, Query}, Node).

-spec query(query(), atom(), term()) -> {ok, term(), term()} | {ok, term()}.
query(Query, Node, Transaction) ->
  aqlparser:parse({str, Query}, Node, Transaction).
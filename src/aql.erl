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
-type query() :: list().

%% API
-export([start/0, stop/0]).

-export([start_shell/0]).
-export([query/1, query/2]).
-export([read_file/1, read_file/2]).

-spec start() -> {'ok', [atom()]}.
start() ->
  {ok, _} = application:ensure_all_started(aql).

-spec stop() -> ok.
stop() ->
  application:stop(aql).

-spec start_shell() -> term().
start_shell() ->
  start(),
  aqlparser:start_shell().

-spec query(query()) -> {ok, term(), term()} | {ok, term()} | {error, term(), term()}.
query(Query) ->
  aqlparser:parse({str, Query}).

-spec query(query(), term()) -> {ok, term(), term()} | {ok, term()} | {error, term(), term()}.
query(Query, Transaction) ->
  aqlparser:parse({str, Query}, Transaction).

-spec read_file(term()) -> {ok, term(), term()} | {ok, term()}.
read_file(Filename) ->
  aqlparser:parse({file, Filename}).

-spec read_file(term(), term()) -> {ok, term(), term()} | {ok, term()}.
read_file(Filename, Transaction) ->
  aqlparser:parse({file, Filename}, Transaction).
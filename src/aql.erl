%%%-------------------------------------------------------------------
%%% @author Pedro Lopes
%%% @doc The AQL interface, responsible for the AQL application.
%%%      The AQL application is composed of a HTTP server, responsible
%%%      for receiving queries via an HTTP endpoint. Alternatively,
%%%      operations may be issued to the application via RPC.
%%%
%%%      Interface:
%%%      - start():
%%%           Starts the AQL application with the default
%%%           configuration;
%%%      - stop():
%%%           Stops the AQL application;
%%%      - start_shell():
%%%           Starts the AQL shell with default configuration;
%%%      - query(Query):
%%%           Issues a query to the AQL.
%%%           The query is performed on a single transaction,
%%%           and committed or aborted after realization;
%%%      - query(Query, Transaction):
%%%           Issues a query to the AQL, on a given
%%%           transaction;
%%%      - read_file(Filename):
%%%           Reads a bath of queries from a file and issues them to
%%%           the AQL. Each query is executed in an individual
%%%           transaction.
%%%      - read_file(Filename, Transaction):
%%%           Reads a bath of queries from a file and issues them to
%%%           the AQL. The batch is executed in a single transaction.
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
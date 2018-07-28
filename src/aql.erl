%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc The AQL interface, responsible for the AQL application.
%%%      The AQL application is composed on an HTTP server, responsible
%%%      for receiving queries via an HTTP endpoint, and an Antidote
%%%      server, responsible for establishing communication with an
%%%      Antidote node through Protocol Buffers (PBs).
%%%
%%%      Interface:
%%%      - start():
%%%           Starts the AQL application with the default
%%%           configuration;
%%%      - start(pb_address, pb_port):
%%%           Starts the AQL application with configured PB address
%%%           and PB port;
%%%      - stop():
%%%           Stops the AQL application;
%%%      - start_shell():
%%%           Starts the AQL shell with default configuration;
%%%      - start_shell(node):
%%%           Starts the AQL shell configured with the name of an
%%%           Antidote node;
%%%      - start_shell(pb_address, pb_port):
%%%           Starts the AQL shell configured with a PB address and a
%%%           PB port;
%%%      - query(query):
%%%           Issues a query to the AQL with default configuration.
%%%           The query is performed on a single transaction,
%%%           committed or aborted after realization;
%%%      - query(query, transaction):
%%%           Issues a query on a given transaction.
%%%
%%% @end
%%% Created : 28. jul 2018 12:22
%%%-------------------------------------------------------------------
-module(aql).

%% Types
-type pb_address() :: list().
-type pb_port() :: non_neg_integer().
-type antidote_node() :: atom().
-type query() :: list().

%% API
-export([start/0, start/2, stop/0]).

-export([start_shell/0, start_shell/1, start_shell/2]).
-export([query/1, query/2]).

-spec start() -> ok.
start() ->
  {ok, _} = application:ensure_all_started(aql).

-spec start(pb_address(), pb_port()) -> ok.
start(PBAddress, PBPort) ->
  ok = application:set_env(aql, pb_address, PBAddress, [{persistent, true}]),
  ok = application:set_env(aql, pb_port, PBPort, [{persistent, true}]),
  start().

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

-spec start_shell(pb_address(), pb_port()) -> term().
start_shell(PBAddress, PBPort) ->
  start(PBAddress, PBPort),
  aqlparser:start_shell().

-spec query(query()) -> {ok, term(), term()} | {ok, term()}.
query(Query) ->
  aqlparser:parse({str, Query}, ignore).

-spec query(query(), term()) -> {ok, term(), term()} | {ok, term()}.
query(Query, Transaction) ->
  aqlparser:parse({str, Query}, ignore, Transaction).
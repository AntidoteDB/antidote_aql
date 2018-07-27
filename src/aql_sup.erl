%%%-------------------------------------------------------------------
%% @doc aql top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(aql_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(ANTIDOTE_OPS, ["127.0.0.1", 8087]).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  ElliOpts = [{callback, aql_http_handler}, {port, 3002}],
  ElliSpec = {
    fancy_http,
    {elli, start_link, [ElliOpts]},
    permanent,
    5000,
    worker,
    [elli]},

  AntidoteSpec = {
    antidote,
    {antidote, start_link, ?ANTIDOTE_OPS},
    permanent,
    5000,
    worker,
    [antidote]
  },

%%  ShellSpec = {
%%    aqlparser,
%%    {aqlparser, start_link, []},
%%    permanent,
%%    5000,
%%    worker,
%%    [aqlparser]
%%  },

  {ok, {{one_for_one, 5, 10}, [ElliSpec, AntidoteSpec]}}.

%%====================================================================
%% Internal functions
%%====================================================================

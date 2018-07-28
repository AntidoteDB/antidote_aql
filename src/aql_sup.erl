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
-define(DEFAULT_PB_ADDRESS, "127.0.0.1").
-define(DEFAULT_PB_PORT, 8087).

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

  PBAddress =
    case application:get_env(aql, pb_address) of
      {ok, Address} -> Address;
      undefined -> ?DEFAULT_PB_ADDRESS
    end,
  PBPort =
    case application:get_env(aql, pb_port) of
      {ok, Port} -> Port;
      undefined -> ?DEFAULT_PB_PORT
    end,

  AntidoteSpec = {
    antidote,
    {antidote, start_link, [PBAddress, PBPort]},
    permanent,
    5000,
    worker,
    [antidote]
  },

  {ok, {{one_for_one, 5, 10}, [ElliSpec, AntidoteSpec]}}.

%%====================================================================
%% Internal functions
%%====================================================================

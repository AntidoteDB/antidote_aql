%%%-------------------------------------------------------------------
%%% @author João Sousa, Pedro Lopes
%%% @doc Utilities for unit tests.
%%% @end
%%%-------------------------------------------------------------------

-module(eutils).

-include("parser.hrl").

%% API
-export([create_table_aux/0]).

create_table_aux() ->
  {ok, Tokens, _} = scanner:string("CREATE UPDATE-WINS TABLE Universities (WorldRank INT PRIMARY KEY, InstitutionId VARCHAR DEFAULT 'aaa', NationalRank COUNTER_INT CHECK (NationalRank > 5));"),
  {ok, [?CREATE_CLAUSE(Table)]} = parser:parse(Tokens),
  table:prepare_table(Table, [], undefined).

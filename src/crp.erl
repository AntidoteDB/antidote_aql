%%%-------------------------------------------------------------------
%%% @author João Sousa, Pedro Lopes
%%% @doc A module to handle Conflict-Resolution Policies (CRP). These
%%%      CRPs are defined per table basis.
%%% @end
%%%-------------------------------------------------------------------

-module(crp).

-define(ERR_UW_DW, "A table with 'Update-Wins' foreign keys cannot be linked to a table with 'Delete-Wins' foreign keys").

-include("aql.hrl").
-include("types.hrl").

%% API
-export([new/0,
  table_level/1, set_table_level/2,
  dep_level/1, set_dep_level/2,
  p_dep_level/1, set_p_dep_level/2,
  get_rule/1
]).

new() ->
  ?T_CRP(undefined, undefined, undefined).

table_level(?T_CRP(TableLevel, _, _)) -> TableLevel.

dep_level(?T_CRP(_, DepLevel, _)) -> DepLevel.

p_dep_level(?T_CRP(_, _, PDepLevel)) -> PDepLevel.

set_table_level(undefined, CRP) -> CRP;
set_table_level(Rule, ?T_CRP(Rule, _, _) = CRP) -> CRP;
set_table_level(Rule, ?T_CRP(undefined, DepLevel, PDepLevel)) ->
  ?T_CRP(Rule, DepLevel, PDepLevel);
set_table_level(_, ?T_CRP(TableLevel, _, _)) ->
  ErrorMsg = io_lib:format("Table level already set to ~p", [TableLevel]),
  throw(lists:flatten(ErrorMsg)).

set_dep_level(undefined, CRP) -> CRP;
set_dep_level(Rule, ?T_CRP(_, Rule, _) = CRP) -> CRP;
set_dep_level(Rule, ?T_CRP(TableLevel, undefined, PDepLevel)) ->
  ?T_CRP(TableLevel, Rule, PDepLevel);
set_dep_level(_, ?T_CRP(_, DepLevel, _)) ->
  ErrorMsg = io_lib:format("Dependency level already set to ~p", [DepLevel]),
  throw(lists:flatten(ErrorMsg)).

set_p_dep_level(undefined, CRP) -> CRP;
set_p_dep_level(Rule, ?T_CRP(_, _, Rule) = CRP) -> CRP;
set_p_dep_level(Rule, ?T_CRP(TableLevel, DepLevel, undefined)) ->
  ?T_CRP(TableLevel, DepLevel, Rule);
set_p_dep_level(?ADD_WINS, ?T_CRP(_, ?REMOVE_WINS, _)) ->
  throw(?ERR_UW_DW);
set_p_dep_level(_, ?T_CRP(_, _, PDepLevel)) ->
  ErrorMsg = io_lib:format("Parent level already set to ~p", [PDepLevel]),
  throw(lists:flatten(ErrorMsg)).

get_rule(Crp) ->
  ?T_CRP(TableLevel, DepLevel, PDepLevel) = Crp,
  Rule1 = rule_table_level(TableLevel),
  Rule2 = rule_dep_level(DepLevel, Rule1),
  rule_p_dep_level(PDepLevel, Rule2).

rule_table_level(?ADD_WINS) -> [ipa:delete(), ipa:insert()];
rule_table_level(?REMOVE_WINS) -> [ipa:insert(), ipa:delete()];
rule_table_level(Crp) -> rule_table_level(table_level(Crp)).

rule_dep_level(undefined, Rule) -> Rule;
rule_dep_level(?ADD_WINS, Rule) -> Rule;
rule_dep_level(?REMOVE_WINS, Rule) -> Rule;
rule_dep_level(?NO_CONCURRENCY, Rule) -> Rule;
rule_dep_level(Crp, Rule) -> rule_dep_level(dep_level(Crp), Rule).

rule_p_dep_level(undefined, Rule) -> Rule;
rule_p_dep_level(?ADD_WINS, Rule) -> lists:append(Rule, [ipa:touch()]);
rule_p_dep_level(?REMOVE_WINS, Rule) -> Rule;
rule_p_dep_level(?NO_CONCURRENCY, Rule) -> Rule;
rule_p_dep_level(Crp, Rule) -> rule_p_dep_level(p_dep_level(Crp), Rule).

%% @author joao
%% @author Pedro Lopes
%% @doc @todo Add description to antidote.


-module(antidote_handler).

-define(LOCK_WAIT_TIME, 0).
-define(LOCK_WAIT_TIME_ES, 0).
%-define(LOCK_WAIT_TIME, 10).
%-define(LOCK_WAIT_TIME_ES, 10).

-define(NODE, 'antidote@127.0.0.1').

-type key() :: atom().
-type crdt_type() :: antidote_crdt_counter_b % valid antidote_crdt types
| antidote_crdt_counter_pn
| antidote_crdt_counter_fat
| antidote_crdt_map_go
| antidote_crdt_set_go
% | antidote_crdt_integer = deprecated
| antidote_crdt_register_lww
| antidote_crdt_map_rr
| antidote_crdt_register_mv
| antidote_crdt_set_aw
% | antidote_crdt_rga = deprecated
| antidote_crdt_set_rw
| antidote_crdt_flag_ew
| antidote_crdt_flag_dw
| antidote_crdt_index
| antidote_crdt_index_p.

-type bucket() :: atom().
-type bound_object() :: {key(), crdt_type(), bucket()}.
-type bound_objects() :: [bound_object()] | bound_object().
-type vectorclock() :: term(). % check antidote project
-type snapshot_time() :: vectorclock() | ignore.

-record(tx_id, {
  local_start_time :: clock_time(),
  server_pid :: atom() | pid()
}).

-type txid() :: #tx_id{}. % check antidote project
-type clock_time() :: non_neg_integer().
-type reason() :: term().
-type properties() :: term() | [].

%% Filtering types
-type filter() :: [filter_content()].

-type filter_content() :: table_filter() | projection_filter() | conditions_filter().

-type table_name() :: atom() | list().
-type table_filter() :: {tables, [table_name()]}.

-type column_name() :: atom() | list().
-type projection_filter() :: {projection, [column_name()]}.

-type comparison() :: atom().
-type value() :: term().
-type condition() :: {column_name(), comparison(), value()}.

-type conditions_filter() :: {conditions, [condition()]}.

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_transaction/0, start_transaction/2,
  read_objects/2,
  commit_transaction/1, abort_transaction/1,
  update_objects/2,
  query_objects/2]).

%% Locks
-export([get_locks/2,
  get_locks/3,
  release_locks/2]).

-export([handleUpdateError/1]).

-spec start_transaction() -> {ok, txid()} | {error, reason()}.
start_transaction() ->
  start_transaction(ignore, []).

-spec start_transaction(snapshot_time(), properties()) -> {ok, txid()} | {error, reason()}.
start_transaction(Snapshot, Props) ->
  %case call(start_transaction, [Snapshot, Props]) of
  case antidote:start_transaction(Snapshot, Props) of
    {ok, TxId} ->
      {ok, TxId};
    Else ->
      Else
  end.

-spec commit_transaction(txid()) -> {ok, vectorclock()} | {error, reason()}.
commit_transaction(TxId) ->
  try
    %call(commit_transaction, [TxId])
    antidote:commit_transaction(TxId)
  of
    Res -> Res
  catch
    _:Exception ->
      {error, Exception}
    %Reason ->
    %  {error, Reason}
  end.

-spec abort_transaction(txid()) -> {ok, vectorclock()} | {error, reason()}.
abort_transaction(TxId) ->
  try
    %call(abort_transaction, [TxId])
    antidote:abort_transaction(TxId)
  of
    Res -> Res
  catch
    _:Exception ->
      {error, Exception}
    %Reason ->
    %  {error, Reason}
  end.

-spec read_objects(bound_objects(), txid()) -> {ok, [term()]}.
read_objects(Objects, TxId) when is_list(Objects) ->
  %call(read_objects, [Objects, TxId]);
  antidote:read_objects(Objects, TxId);
read_objects(Object, Ref) ->
  read_objects([Object], Ref).

-spec update_objects(bound_objects(), txid()) -> ok | {error, reason()}.
update_objects(Objects, TxId) when is_list(Objects) ->
  %call(update_objects, [Objects, TxId]);
  antidote:update_objects(Objects, TxId);
update_objects(Object, Ref) ->
  update_objects([Object], Ref).

-spec query_objects(filter(), txid()) -> {ok, [term()]} | {error, reason()}.
query_objects(Filter, TxId) ->
  %call(query_objects, [Filter, TxId]).
  antidote:query_objects(Filter, TxId).

-spec get_locks([key()], txid()) -> {ok, [snapshot_time()]} | {missing_locks, [key()]} | {locks_in_use, [txid()]}.
get_locks(Locks, TxId) ->
  %Res = call(get_locks, [?LOCK_WAIT_TIME, Locks, TxId]),
  Res = antidote:get_locks(?LOCK_WAIT_TIME, Locks, TxId),
  case Res of
    {ok, _} -> ok;
    {missing_locks, Keys} ->
      ErrorMsg = io_lib:format("One or more locks are missing: ~p", [Keys]),
      throw(lists:flatten(ErrorMsg));
    {locks_in_use, UsedLocks} ->
      FilterNotThisTx =
        lists:filter(fun({TxId0, _LockList}) -> TxId0 /= TxId end, UsedLocks),
      case FilterNotThisTx of
        [] -> ok;
        _ ->
          ErrorMsg = io_lib:format("One or more exclusive locks are being used by other transactions: ~p", [FilterNotThisTx]),
          throw(lists:flatten(ErrorMsg))
      end;
    {locks_not_available, NALocks} ->
      ErrorMsg = io_lib:format("One or more locks are not available: ~p", [NALocks]),
      throw(lists:flatten(ErrorMsg))
  end.

-spec get_locks([key()], [key()], txid()) -> {ok, [snapshot_time()]} | {missing_locks, [key()]} | {locks_in_use, [txid()]}.
get_locks(SharedLocks, ExclusiveLocks, TxId) ->
  %Res = call(get_locks, [?LOCK_WAIT_TIME_ES, SharedLocks, ExclusiveLocks, TxId]),
  Res = antidote:get_locks(?LOCK_WAIT_TIME_ES, SharedLocks, ExclusiveLocks, TxId),
  case Res of
    {ok, _} -> ok;
    {missing_locks, Keys} ->
      ErrorMsg = io_lib:format("One or more locks are missing: ~p", [Keys]),
      throw(lists:flatten(ErrorMsg));
    {locks_in_use, {UsedExclusive, _UsedShared}} ->
      FilterNotThisTx =
        lists:filter(fun({TxId0, _LockList}) -> TxId0 /= TxId end, UsedExclusive),
      case FilterNotThisTx of
        [] -> ok;
        _ ->
          ErrorMsg = io_lib:format("One or more exclusive locks are being used by other transactions: ~p", [FilterNotThisTx]),
          throw(lists:flatten(ErrorMsg))
      end;
    {locks_not_available, {NAExclusive, _NAShared}} ->
      ErrorMsg = io_lib:format("One or more exclusive locks are not available: ~p", [NAExclusive]),
      throw(lists:flatten(ErrorMsg))
  end.

-spec release_locks(locks | es_locks, txid()) -> ok.
release_locks(Type, TxId) ->
  %Res = call(release_locks, [Type, TxId]),
  Res = antidote:release_locks(Type, TxId),
  Res.

handleUpdateError({{{badmatch, {error, no_permissions}}, _}, _}) ->
  %{error, {{badmatch,{error,no_permissions}}
  "A numeric invariant has been breached.";
handleUpdateError(Msg) ->
  Msg.


%% ====================================================================
%% Internal functions
%% ====================================================================

% TODO keep for local testing (i.e. tests that don't use the application)
%call(Function, Args) ->
%	rpc:call(?NODE, antidote, Function, Args).

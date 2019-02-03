%%%-------------------------------------------------------------------
%%% @author Pedro Lopes
%%% @doc A module to handle strong consistency properties of AQL.
%%%      Simple, shared, or exclusive locks can be acquired or revoked
%%%      through the underlying Antidote node.
%%% @end
%%%-------------------------------------------------------------------

-module(lock_manager).

-include("aql.hrl").

%% API
-export([acquire_lock/2,
  release_locks/1]).
-export([acquire_exclusive_lock/2,
  acquire_shared_lock/2,
  acquire_es_locks/3,
  release_es_locks/1]).

%%====================================================================
%% Simple locks
%%====================================================================
acquire_lock(Locks, TxId) when is_list(Locks) ->
  antidote_handler:get_locks(Locks, TxId);
acquire_lock(Lock, TxId) ->
  acquire_lock([Lock], TxId).

release_locks(TxId) ->
  antidote_handler:release_locks(locks, TxId).

%%====================================================================
%% Exclusive/shared locks
%%====================================================================

acquire_exclusive_lock(ExclusiveLocks, TxId) when is_list(ExclusiveLocks) ->
  antidote_handler:get_locks([], ExclusiveLocks, TxId);
acquire_exclusive_lock(ExclusiveLock, TxId) ->
  acquire_exclusive_lock([ExclusiveLock], TxId).

acquire_shared_lock(SharedLocks, TxId) when is_list(SharedLocks) ->
  antidote_handler:get_locks(SharedLocks, [], TxId);
acquire_shared_lock(SharedLock, TxId) ->
  acquire_shared_lock([SharedLock], TxId).

acquire_es_locks(Exclusive, Shared, TxId)
  when is_list(Exclusive) andalso is_list(Shared) ->
  antidote_handler:get_locks(Shared, Exclusive, TxId).

release_es_locks(TxId) ->
  antidote_handler:release_locks(es_locks, TxId).

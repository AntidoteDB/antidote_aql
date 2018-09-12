%%%-------------------------------------------------------------------
%%% @author pedrolopes
%%% @doc
%%%
%%% @end
%%% Created : 07. set 2018 13:46
%%%-------------------------------------------------------------------
-module(lock_manager).
-author("pedrolopes").

-include("aql.hrl").

%% API
-export([acquire_lock/2,
  release_locks/1]).
-export([acquire_exclusive_lock/2,
  acquire_shared_lock/2,
  acquire_es_locks/3,
  release_es_locks/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%        Simple Locks        %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
acquire_lock(Locks, TxId) when is_list(Locks) ->
  antidote_handler:get_locks(Locks, TxId);
acquire_lock(Lock, TxId) ->
  acquire_lock([Lock], TxId).

release_locks(TxId) ->
  antidote_handler:release_locks(locks, TxId).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%   Exclusive/Shared Locks   %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

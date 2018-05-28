#!/usr/bin/env escript
%%! -smp enable -name populate@127.0.0.1 -setcookie antidote

%-mode(compile).

-define(ENTRY_TYPE, {antidote_crdt_register_lww, [assign]}).

main([Node, NumEntries, Key, CRDT, Bucket]) ->
	try
		{list_to_atom(Node), list_to_integer(NumEntries), list_to_atom(Key),
		 list_to_atom(CRDT), list_to_binary(Bucket)}
	of
		{NodeAtom, NEntries, KeyAtom, CRDTAtom, BucketBin} ->
			io:format("Parameters to be used:~n"),
			io:format("  Antidote node: ~p~n", [NodeAtom]),
			io:format("  Entries: ~p~n", [NEntries]),
			io:format("  Key: ~p~n", [KeyAtom]),
			io:format("  Index CRDT: ~p~n", [CRDTAtom]),
			io:format("  Bucket: ~p~n", [BucketBin]),
			io:format("  Index entry type: ~p~n", [?ENTRY_TYPE]),
			
			{ok, TxId} = rpc:call(NodeAtom, antidote, start_transaction, [ignore, []]),
			ok = fill_database(NodeAtom, NEntries, KeyAtom, CRDTAtom, BucketBin, TxId),
			
			{ok, _} = rpc:call(NodeAtom, antidote, commit_transaction, [TxId]),
			
			io:format("Index populated successfully!~n")
	catch
		Exception:Reason -> {caught, Exception, Reason}
	end.
	
fill_database(Node, NumEntries, Key, CRDT, Bucket, TxId) ->
	Seq = lists:seq(1, NumEntries),
	BoundObject = {Key, CRDT, Bucket},
	io:format("Updating bounded object: ~p...~n", [BoundObject]),
	lists:foreach(fun(EntryVal) ->
		EntryPK = random_string(10),
		Op = to_operation(?ENTRY_TYPE, EntryPK, EntryVal),
  		Update = {BoundObject, update, Op},
  		ok = rpc:call(Node, antidote, update_objects, [[Update], TxId])
  		%io:format("Updated entry: Key = ~p, Entry = ~p~n", [Key, {EntryPK, EntryVal}])
	end, Seq),
	ok.

random_string(Len) ->
    Chrs = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    ChrsSize = size(Chrs),
    F = fun(_, R) -> [element(rand:uniform(ChrsSize), Chrs) | R] end,
    lists:foldl(F, "", lists:seq(1, Len)).
		
to_operation({CRDT, [Op]}, Key, Val) ->
  {CRDT, Key, {Op, Val}}.

#!/usr/bin/env escript
%%! -smp enable -name populate@127.0.0.1 -setcookie antidote

%-mode(compile).

-define(ENTRY_TYPE, {antidote_crdt_register_lww, [assign]}).

main([Nodes, NumEntries, Key, CRDT, Bucket]) ->
	try
		{nodes_to_atoms(Nodes), list_to_integer(NumEntries), list_to_atom(Key),
		 list_to_atom(CRDT), list_to_binary(Bucket)}
	of
		{NodeAtoms, NEntries, KeyAtom, CRDTAtom, BucketBin} ->
			io:format("Parameters to be used:~n"),
			io:format("  Antidote node: ~p~n", [NodeAtoms]),
			io:format("  Entries: ~p~n", [NEntries]),
			io:format("  Key: ~p~n", [KeyAtom]),
			io:format("  Index CRDT: ~p~n", [CRDTAtom]),
			io:format("  Bucket: ~p~n", [BucketBin]),
			io:format("  Index entry type: ~p~n", [?ENTRY_TYPE]),

			ok = fill_database(NodeAtoms, NEntries, KeyAtom, CRDTAtom, BucketBin),			
			
			io:format("Index populated successfully!~n")
	catch
		Exception:Reason -> {caught, Exception, Reason}
	end.
	
fill_database(Nodes, NumEntries, Key, CRDT, Bucket) ->
	Seq = lists:seq(1, NumEntries),
	NLists = NumEntries div length(Nodes) + 1,
	ListOfLists = split_list(Seq, NLists),
	
	BoundObject = {Key, CRDT, Bucket},
	io:format("Updating bounded object: ~p...~n", [BoundObject]),
	
	lists:foldl(fun(List, CurrNode) ->
		TargetNode = lists:nth(CurrNode, Nodes),
		{ok, TxId} = rpc:call(TargetNode, antidote, start_transaction, [ignore, []]),
		
		Ops = lists:map(fun(EntryVal) ->
			EntryPK = random_string(10),
			Op = to_operation(?ENTRY_TYPE, EntryPK, EntryVal),
			Op
		end, List),
		
		Update = {BoundObject, update, Ops},
	  	ok = rpc:call(TargetNode, antidote, update_objects, [[Update], TxId]),
	  	{ok, _} = rpc:call(TargetNode, antidote, commit_transaction, [TxId]),
	  	
		CurrNode + 1
	end, 1, ListOfLists),
	ok.
	
nodes_to_atoms(Nodes) ->
	TrimStr = string:sub_string(Nodes, 2, length(Nodes) - 1),
	SplitNodes = string:tokens(TrimStr, ","),
	lists:map(fun(Node) -> erlang:list_to_atom(Node) end, SplitNodes).
	
split_list(List, N) ->
	split_list(List, N, []).
split_list([], _N, Acc) -> Acc;
split_list(CurrList, N, Acc) when length(CurrList) < N ->
	Acc ++ [CurrList];
split_list(CurrList, N, Acc) ->
	{List1, List2} = lists:split(N, CurrList),
	split_list(List2, N, Acc ++ [List1]).

random_string(Len) ->
    Chrs = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    ChrsSize = size(Chrs),
    F = fun(_, R) -> [element(rand:uniform(ChrsSize), Chrs) | R] end,
    lists:foldl(F, "", lists:seq(1, Len)).
		
to_operation({CRDT, [Op]}, Key, Val) ->
  {CRDT, Key, {Op, Val}}.

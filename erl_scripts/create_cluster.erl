#!/usr/bin/env escript
%%! -smp enable -sname cluster -mnesia debug verbose

-mode(compile).

main(ConfigFile) ->
	%% Receives a cluster.config file
	ReadParameters = read_from_file(hd(ConfigFile), [read], []),
	
	{antidote_nodes, Nodes} = lists:keyfind(antidotec_nodes, 1, ReadParameters),
	
	%% Starting processes
	lists:foreach(fun(Node) ->
		ok = rpc:call(Node, inter_dc_manager, start_bg_processes, [stable])
	end, Nodes),
	
	%% Getting node descriptors
	Descriptors = lists:map(fun(Node) ->
		{ok, Desc} = rpc:call(Node, inter_dc_manager, get_descriptor, []),
		Desc
	end, Nodes),
	
	%% Synchronize nodes
	lists:foreach(fun(Node) ->
		rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
	end, Nodes).
	
%% ===================================================================
%% Internal functions
%% ===================================================================
read_from_file(FileName, Mode, Acc0) ->
    {ok, Device} = file:open(FileName, Mode),
    for_each_line(Device, Acc0).
 
for_each_line(Device, Acc) ->
    case io:get_line(Device, "") of
        eof  ->
        	file:close(Device), Acc;
        Line when Line =/= "\n" ->
			case string:chr(Line, $%) of
				1 ->
					for_each_line(Device, Acc);
				_ ->
					Tuple = to_tuple(Line),
					NewAcc = Acc ++ [Tuple],
					for_each_line(Device, NewAcc)
			end;
		_ ->
			for_each_line(Device, Acc)
    end.
    
to_tuple(String) ->
	{ok, Tokens, _} = erl_scan:string(String),
	{ok, Tuple} = erl_parse:parse_term(Tokens),
	Tuple.

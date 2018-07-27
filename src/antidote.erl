%% @author joao
%% @doc @todo Add description to antidote.


-module(antidote).
-behaviour(gen_server).

-type key() :: atom().
-type crdt_type() :: antidote_crdt_counter_b % valid antidote_crdt types
									| antidote_crdt_counter_pn
									| antidote_crdt_counter_fat
									| antidote_crdt_map_go
									| antidote_crdt_set_go
									| antidote_crdt_register_lww
									| antidote_crdt_map_rr
									| antidote_crdt_register_mv
									| antidote_crdt_set_aw
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
-type ref() :: {node_ref(), txid()}.
-type txid() :: term(). % check antidote project
-type node_ref() :: term().
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

-record(state, {node = 'antidote@127.0.0.1', address, port, pid}).

%% ====================================================================
%% gen_server functions
%% ====================================================================
-export([init/1,
        start_link/0,
        start_link/2,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_transaction/1, start_transaction/3,
				read_objects/2,
				commit_transaction/1, abort_transaction/1,
		 		update_objects/2,
				query_objects/2]).

-export([handleBadRpc/1]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
start_link(PBAddress, PBPort) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [PBAddress, PBPort], []).

init([]) ->
  {ok, #state{}};
init([Node]) ->
  {ok, #state{node = Node}};
init([NodeAddress, NodePort]) ->
  %State = #state{address = NodeAddress, port = NodePort},
  %ConnectTimeout = infinity, % timeout of TCP connection
  %KeepAlive = false,
  %case gen_tcp:connect(NodeAddress, NodePort,
  %  [binary, {active, once}, {packet, 4},
  %    {keepalive, KeepAlive}],
  %  ConnectTimeout) of
  %  {ok, Sock} ->
  %    {ok, State#state{pid = Sock}};
  %  {error, Reason} ->
  %    {stop, {tcp, Reason}}
  %end.
  {ok, Pid} = antidotec_pb_socket:start_link(NodeAddress, NodePort),
  {ok, #state{address = NodeAddress, port = NodePort, pid = Pid}}.

handle_call({start_transaction, Snapshot, Props}, _From, State = #state{pid = Pid}) ->
  Clock = term_to_binary(Snapshot),
  {ok, TxId} = antidotec_pb:start_transaction(Pid, Clock, Props),
  {reply, {ok, TxId}, State};

handle_call({commit_transaction, TxId}, _From, State = #state{pid = Pid}) ->
  {ok, TimeStamp} = antidotec_pb:commit_transaction(Pid, TxId),
  {reply, {ok, TimeStamp}, State};

handle_call({abort_transaction, TxId}, _From, State = #state{pid = Pid}) ->
  Result = antidotec_pb:abort_transaction(Pid, TxId),
  {reply, Result, State};

handle_call({read_objects, Objects, TxId}, _From, State = #state{pid = Pid}) ->
  EncObjects = encode_reads(Objects),
  case antidotec_pb:read_values(Pid, EncObjects, TxId) of
    {ok, Result} ->
      {reply, {ok, decode_reads(Result)}, State};
    Other ->
      {reply, Other, State}
  end;

handle_call({update_objects, Objects, TxId}, _From, State = #state{pid = Pid}) ->
  EncObjects = encode_updates(Objects),
  Result = antidotec_pb:update_objects(Pid, EncObjects, TxId),
  {reply, Result, State};

handle_call({query_objects, Filter, TxId}, _From, State = #state{pid = Pid}) ->
  EncFilter = term_to_binary(Filter),
  case antidotec_pb:query_objects(Pid, EncFilter, TxId) of
    {ok, Result} ->
      {reply, {ok, binary_to_term(Result)}, State};
    Other ->
      {reply, Other, State}
  end.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

-spec start_transaction(node_ref()) -> {ok, ref()} | {error, reason()}.
start_transaction(Node) ->
	start_transaction(Node, ignore, []).

-spec start_transaction(node_ref(), snapshot_time(), properties()) -> {ok, ref()} | {error, reason()}.
start_transaction(Node, Snapshot, Props) ->
	%case call(Node, start_transaction, [Snapshot, Props]) of
  case gen_server:call(?MODULE, {start_transaction, Snapshot, Props}, infinity) of
		{ok, TxId} ->
			{ok, {Node, TxId}};
		Else ->
			Else
	end.

-spec commit_transaction(ref()) -> {ok, vectorclock()} | {error, reason()}.
commit_transaction({_Node, TxId}) ->
	%Res = call(Node, commit_transaction, [TxId]),
  Res = gen_server:call(?MODULE, {commit_transaction, TxId}, infinity),
	Res.

-spec abort_transaction(ref()) -> {ok, vectorclock()} | {error, reason()}.
abort_transaction({_Node, TxId}) ->
	%Res = call(Node, abort_transaction, [TxId]),
  Res = gen_server:call(?MODULE, {abort_transaction, TxId}, infinity),
	Res.

-spec read_objects(bound_objects(), ref()) -> {ok, [term()]}.
read_objects(Objects, {_Node, TxId}) when is_list(Objects) ->
	%call(Node, read_objects, [Objects, TxId]);
  Res = gen_server:call(?MODULE, {read_objects, Objects, TxId}, infinity),
  Res;
read_objects(Object, Ref) ->
	read_objects([Object], Ref).

-spec update_objects(bound_objects(), ref()) -> ok | {error, reason()}.
update_objects(Objects, {_Node, TxId}) when is_list(Objects) ->
	%call(Node, update_objects, [Objects, TxId]);
  gen_server:call(?MODULE, {update_objects, Objects, TxId}, infinity);
update_objects(Object, Ref) ->
	update_objects([Object], Ref).

-spec query_objects(filter(), txid()) -> {ok, [term()]} | {error, reason()}.
query_objects(Filter, {_Node, TxId}) ->
	%call(Node, query_objects, [Filter, TxId]).
  gen_server:call(?MODULE, {query_objects, Filter, TxId}, infinity).

handleBadRpc({'EXIT', {{{badmatch, {error, no_permissions}}, _}}}) ->
	{"Constraint Breach", "A numeric invariant has been breached."};
handleBadRpc(_Msg) ->
	{"Internal Error", "Unexpected error"}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%call(Node, Function, Args) ->
%	rpc:call(Node, antidote, Function, Args).

encode_reads(Objects) ->
  lists:map(fun(Object) ->
    encode_key(Object)
  end, Objects).

decode_reads(Objects) ->
  lists:map(fun(Object) ->
    decode_object(Object)
  end, Objects).

encode_key({Key, Type, Bucket}) ->
  BinKey = utils:to_binary(Key),
  BinBucket = utils:to_binary(Bucket),
  {BinKey, Type, BinBucket}.

decode_key({EncKey, Type, EncBucket}) ->
  Key = utils:to_atom(EncKey),
  Bucket = utils:to_atom(EncBucket),
  {Key, Type, Bucket}.

decode_object({counter_b, {Incs, Decs}}) ->
  DecIncs =
    orddict:fold(
      fun
        ({_K, K}, V, Acc) ->
          RawK = utils:to_term(K),
          orddict:store({RawK, RawK}, V, Acc);
        (K, V, Acc) ->
          orddict:store(utils:to_term(K), V, Acc)
      end, orddict:new(), Incs),
  DecDecs = orddict:fold(
    fun(K, V, Acc) ->
      orddict:store(utils:to_term(K), V, Acc)
    end, orddict:new(), Decs),
  {DecIncs, DecDecs};
decode_object({counter, Counter}) ->
  Counter;
decode_object({reg, Register}) ->
  utils:to_term(Register);
decode_object({mvreg, MVRegister}) ->
  [utils:to_term(Val) || Val <- MVRegister];
decode_object({flag, Flag}) ->
  Flag;
decode_object({set, Set}) ->
  [utils:to_term(Elem) || Elem <- Set];
decode_object({map, Map}) ->
  [decode_map_entry(Entry) || Entry <- Map];
decode_object({index, Index}) ->
  lists:map(fun({EntryKey, EntryVal}) when is_list(EntryVal) ->
    {utils:to_term(EntryKey), [decode_key(Elem) || Elem <- EntryVal]}
  end, Index).

encode_crdt(antidote_crdt_counter_b) -> counter_b;
encode_crdt(antidote_crdt_counter_pn) -> counter;
encode_crdt(antidote_crdt_counter_fat) -> counter;
encode_crdt(antidote_crdt_register_lww) -> reg;
encode_crdt(antidote_crdt_register_mv) -> mvreg;
encode_crdt(antidote_crdt_flag_ew) -> flag;
encode_crdt(antidote_crdt_flag_dw) -> flag;
encode_crdt(antidote_crdt_set_go) -> set;
encode_crdt(antidote_crdt_set_aw) -> set;
encode_crdt(antidote_crdt_set_rw) -> set;
encode_crdt(antidote_crdt_map_rr) -> map;
encode_crdt(antidote_crdt_map_go) -> map;
encode_crdt(antidote_crdt_index) -> index;
encode_crdt(antidote_crdt_index_p) -> index.

encode_updates(Objects) ->
  lists:map(fun(Object) ->
    {{_, Type, _} = Key, Op, Update} = Object,
    EncKey = encode_key(Key),
    {Op, EncUpdate} = encode_update({encode_crdt(Type), {Op, Update}}),
    {EncKey, Op, EncUpdate}
  end, Objects).

encode_update({counter_b, {increment, {Value, Actor}}}) ->
  {increment, {Value, term_to_binary(Actor)}};
encode_update({counter_b, {decrement, {Value, Actor}}}) ->
  {decrement, {Value, term_to_binary(Actor)}};
encode_update({counter_b, {transfer, {Value, To, Actor}}}) ->
  {transfer, {Value, term_to_binary(To), term_to_binary(Actor)}};
encode_update({counter, CounterUpd}) ->
  CounterUpd;
encode_update({reg, {assign, Value}}) ->
  {assign, term_to_binary(Value)};
encode_update({mvreg, {assign, Value}}) ->
  {assign, term_to_binary(Value)};
encode_update({mvreg, {reset, {}} = MVRegUpd}) ->
  MVRegUpd;
encode_update({flag, FlagUpd}) ->
  FlagUpd;
encode_update({set, {reset, {}} = SetUpd}) ->
  SetUpd;
encode_update({set, {SetOp, Elems}}) when is_list(Elems) ->
  {SetOp, [term_to_binary(Elem) || Elem <- Elems]};
encode_update({set, {SetOp, Elem}}) ->
  {SetOp, term_to_binary(Elem)};
encode_update({map, {update, Updates}}) when is_list(Updates) ->
  {update, [encode_map_entry_update(Update) || Update <- Updates]};
encode_update({map, {update, Update}}) ->
  {update, encode_map_entry_update(Update)};
encode_update({map, {remove, Removes}}) when is_list(Removes) ->
  {remove, [encode_map_entry_remove(Remove) || Remove <- Removes]};
encode_update({map, {remove, Remove}}) ->
  {remove, encode_map_entry_remove(Remove)};
encode_update({map, {batch, {Updates, Removes}}}) ->
  EncUpdates = [encode_map_entry_update(Update) || Update <- Updates],
  EncRemoves = [encode_map_entry_remove(Remove) || Remove <- Removes],
  {batch, {EncUpdates, EncRemoves}};
encode_update({map, {reset, {}} = MapUpd}) ->
  MapUpd;
encode_update({index, {update, Ops}}) when is_list(Ops) ->
  EncOps =
    lists:map(fun(Op) ->
      {update, EncOp} = encode_update({index, {update, Op}}),
      EncOp
    end, Ops),
  {update, EncOps};
encode_update({index, {update, {Key, Ops}}}) when is_list(Ops) ->
  EncKey = term_to_binary(Key),
  EncOps =
    lists:map(fun
                ({FName, FType, {assign, {K, T, B}}}) ->
                  {FName, FType, {assign, encode_key({K, T, B})}};
                ({FName, FType, Op}) ->
                  {FName, FType, encode_update({encode_crdt(FType), Op})}
              end, Ops),
  {update, {EncKey, EncOps}};
encode_update({index, {update, {Key, {assign, {K, T, B}}}}}) ->
  EncKey = term_to_binary(Key),
  EncOp = {assign, encode_key({K, T, B})},
  {update, {EncKey, EncOp}};
encode_update({index, {update, {Key, Op}}}) ->
  encode_update({index, {update, {Key, [Op]}}});
encode_update({index, {remove, Keys}}) when is_list(Keys) ->
  {remove, [term_to_binary(Key) || Key <- Keys]};
encode_update({index, {remove, Key}}) ->
  {remove, term_to_binary(Key)}.

encode_map_entry_update({{Key, Type}, Op}) ->
  EncKey = term_to_binary(Key),
  EncUpd = encode_update({encode_crdt(Type), Op}),
  {{EncKey, Type}, EncUpd}.

encode_map_entry_remove({Key, Type}) ->
  {term_to_binary(Key), Type}.

decode_map_entry({{Key, Type}, Value}) ->
  DecKey = binary_to_term(Key),
  DecObj = decode_object({encode_crdt(Type), Value}),
  {{DecKey, Type}, DecObj}.
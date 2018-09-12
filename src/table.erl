%% @author Joao
%% @author Pedro Lopes
%% @doc @todo Add description to tables.

-module(table).

-include("parser.hrl").
-include("aql.hrl").
-include("types.hrl").

-define(TABLE_META, ?BOUND_OBJECT('#tables', antidote_crdt_map_go, ?METADATA_BUCKET)).
-define(CRDT_TYPE, antidote_crdt_register_lww).

-export([exec/2]).

-export([read_tables/1,
         write_table/2,
         lookup/2, lookup/3,
         dependants/2,
         prepare_table/3,
         create_table_update/1]).

-export([name/1,
         policy/1,
         columns/1,
	     shadow_columns/1,
	     indexes/1,
	     partition_col/1]).

exec(Table, TxId) ->
	write_table(Table, TxId).

%% ====================================================================
%% Read/Write functions
%% ====================================================================

read_tables(TxId) ->
	{ok, [Tables]} = antidote_handler:read_objects(?TABLE_META, TxId),
	Tables.

write_table(RawTable, TxId) ->
	Tables = read_tables(TxId),
	Table = prepare_table(RawTable, Tables, TxId),
	TableUpdate = create_table_update(Table),
	ok = antidote_handler:update_objects(TableUpdate, TxId).

prepare_table(Table, _Tables, TxId) ->
	{Table1, _Crps} = prepare_cols(Table),
	DepRule = undefined,
  Ops = [],
	case Ops of
		[] -> ok;
		_Else ->
			ok = antidote_handler:update_objects(Ops, TxId)
	end,
	Policy = policy(Table1),
	Policy1 = crp:set_dep_level(DepRule, Policy),
	Table2 = set_policy(Policy1, Table1),
	set_indexes([], Table2).

prepare_cols(Table) ->
	RawCols = columns(Table),
	Builder = lists:foldl(fun columns_builder:put_raw/2, columns_builder:new(), RawCols),
	{Cols, Crps} = columns_builder:build(Builder),
	{set_columns(Cols, Table), Crps}.

create_table_update(Table) ->
	Name = name(Table),
	Op = crdt:assign_lww(Table),
	crdt:single_map_update(?TABLE_META, Name, ?CRDT_TYPE, Op).

lookup(Name, Tables, ErrMsg) ->
	NameAtom = utils:to_atom(Name),
	Res = proplists:get_value(?MAP_KEY(NameAtom, ?CRDT_TYPE), Tables),
	case Res of
		undefined ->
			throw(ErrMsg);
		_Else ->
			Res
	end.

lookup(Name, Tables) when is_list(Tables) ->
	ErrMsg = lists:flatten(io_lib:format("No such table: ~p", [Name])),
	lookup(Name, Tables, ErrMsg);
lookup(Name, TxId) ->
	Tables = read_tables(TxId),
	lookup(Name, Tables).

dependants(TName, Tables) ->
	dependants(TName, Tables, []).

dependants(TName, [{TName, _Table} | Tables], Acc) ->
	dependants(TName, Tables, Acc);
dependants(TName, [{{T1TName, _}, Table} | Tables], Acc) ->
	Fks = shadow_columns(Table),
	Refs = references(TName, Fks, []),
	case Refs of
		[] ->
			dependants(TName, Tables, Acc);
		_Else ->
			dependants(TName, Tables, lists:append(Acc, [{T1TName, Refs}]))
	end;
dependants(_TName, [], Acc) -> Acc.

references(TName, [?T_FK(_, _, TName, _, _) = Fk | Fks], Acc) ->
	references(TName, Fks, lists:append(Acc, [Fk]));
references(TName, [_ | Fks], Acc) ->	references(TName, Fks, Acc);
references(_TName, [], Acc) -> Acc.

%% ====================================================================
%% Table Props functions
%% ====================================================================

name(?T_TABLE(Name, _Policy, _Cols, _SCols, _Idx, _PartCol)) -> Name.

policy(?T_TABLE(_Name, Policy, _Cols, _SCols, _Idx, _PartCol)) -> Policy.

set_policy(Policy, ?T_TABLE(Name, _Policy, Cols, SCols, Idx, PartCol)) ->
	?T_TABLE(Name, Policy, Cols, SCols, Idx, PartCol).

columns(?T_TABLE(_Name, _Policy, Cols, _SCols, _Idx, _PartCol)) -> Cols.

set_columns(Cols, ?T_TABLE(Name, Policy, _Cols, SCols, Idx, PartCol)) ->
	?T_TABLE(Name, Policy, Cols, SCols, Idx, PartCol).

shadow_columns(?T_TABLE(_Name, _Policy, _Cols, SCols, _Idx, _PartCol)) -> SCols.

indexes(?T_TABLE(_Name, _Policy, _Cols, _SCols, Idx, _PartCol)) -> Idx.

set_indexes(Idx, ?T_TABLE(Name, Policy, Cols, SCols, _Idx, PartCol)) ->
	?T_TABLE(Name, Policy, Cols, SCols, Idx, PartCol).

partition_col(?T_TABLE(_Name, _Policy, _Cols, _SCols, _Idx, PartCol)) -> PartCol.

%% ====================================================================
%% Internal functions
%% ====================================================================

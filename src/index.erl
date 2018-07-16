%% @author Joao
%% @author Pedro Lopes
%% @doc @todo Add description to index.

-module(index).

-include("aql.hrl").
-include("types.hrl").
-include("parser.hrl").

-define(INDEX_CRDT, antidote_crdt_index_p).
-define(SINDEX_CRDT, antidote_crdt_index).
-define(INDEX_ENTRY_DT, antidote_crdt_register_lww).
-define(ITAG_CRDT, antidote_crdt_map_go).
-define(ITAG_KEY_CRDT, antidote_crdt_register_mv).
-define(INDEX_PREFIX, "#_").
-define(SINDEX_PREFIX, "#2i_").
-define(TAG_TOKEN, "#__").

-define(BOBJ_FIELD(Val), {bound_obj, Val}).
-define(STATE_FIELD(Val), {state, Val}).
-define(VERSION_FIELD(Val), {version, Val}).
-define(REFS_FIELD(Val), {refs, Val}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([exec/3]).
-export([index/1,
    table/1,
    cols/1]).

-export([primary_index/2, secondary_index/3]).

-export([p_keys/2, p_keys/3,
    s_keys/3, s_keys/4,
    s_keys_formatted/3,
    p_name/1,
    s_name/2,
    p_put/2, p_put/3,
    lookup_index/2]).
-export([tag_name/2,
    tag_key/2, tag_subkey/1,
    tag/5,
    tag_read/4]).
-export([entry_key/1,
    entry_bobj/1,
    entry_state/1,
    entry_version/1,
    entry_refs/1,
    get_ref_by_name/2,
    get_ref_by_spec/2,
    format_refs/1]).

exec({Table, _Tables}, Props, TxId) ->
    %IndexName = index(Props),
    TableName = table(Props),
    IndexCols = cols(Props),
    TIndexes = table:indexes(Table),
    %case has_index(TIndexes, IndexName) of
    %  true ->
    %    throw(lists:concat(["The index ", IndexName, " already exists on table ", TableName]));
    %  _Else ->
    %    ok
    %end,
    case check_keys(Table, IndexCols) of
        [] -> ok;
        List ->
            ErrorMsg = io_lib:format("The columns ~p do not exist in table ~p", [List, TableName]),
            throw(lists:flatten(ErrorMsg))
    end,
    Table2 = set_table_index(lists:append(TIndexes, [Props]), Table),
    TableUpdate = table:create_table_update(Table2),
    ok = antidote:update_objects(TableUpdate, TxId).

index({Name, _TName, _Cols}) -> Name.

table({_Name, TName, _Cols}) -> TName.

cols({_Name, _TName, Cols}) -> Cols.

primary_index(TName, TxId) ->
    BoundObject = crdt:create_bound_object(p_name(TName), ?INDEX_CRDT, ?METADATA_BUCKET),
    {ok, [Res]} = antidote:read_objects(BoundObject, TxId),
    Res.

secondary_index(TName, IndexName, TxId) ->
    BoundObject = crdt:create_bound_object(s_name(TName, IndexName), ?SINDEX_CRDT, ?METADATA_BUCKET),
    {ok, [Res]} = antidote:read_objects(BoundObject, TxId),
    Res.

p_keys(TName, TxId) ->
    PIndex = primary_index(TName, TxId),
    case PIndex of
        {_IdxPol, _DepPol, IndexTree} ->
            lists:map(fun(Entry) ->
                BObj = index:entry_bobj(Entry),
                element:create_key(BObj, TName)
            end, IndexTree);
        [] -> []
    end.

p_keys(TName, Operation, TxId) ->
    BoundObject = crdt:create_bound_object(p_name(TName), ?INDEX_CRDT, ?METADATA_BUCKET),
    {ok, [Res]} = antidote:read_objects({BoundObject, Operation}, TxId),
    case Res of
        {error, _} -> [];
        Value -> Value
    end.

%% Reads a secondary index
s_keys(TName, IndexName, TxId) ->
    secondary_index(TName, IndexName, TxId).

s_keys(TName, IndexName, Operation, TxId) ->
    BoundObject = crdt:create_bound_object(s_name(TName, IndexName), ?SINDEX_CRDT, ?METADATA_BUCKET),
    {ok, [Res]} = antidote:read_objects({BoundObject, Operation}, TxId),
    Res.

s_keys_formatted(TName, IndexName, TxId) ->
    Tables = table:read_tables(TxId),
    Table = table:lookup(TName, Tables),
    IndexData = s_keys(TName, IndexName, TxId),
    {_IndexName, _TableName, [Column]} = lookup_index(IndexName, Table), %% todo support more than one column
    Col = column:s_get(Table, Column),
    Type = column:type(Col),
    Cons = column:constraint(Col),
    case {Type, Cons} of
        {?AQL_COUNTER_INT, ?CHECK_KEY({_Key, ?COMPARATOR_KEY(Comp), Offset})} ->
            lists:map(fun({EntryKey, EntryVal}) ->
                AQLCounterValue = bcounter:from_bcounter(Comp, EntryKey, Offset),
                {AQLCounterValue, EntryVal}
                      end, IndexData);
        _Else ->
            IndexData
    end.

p_name(TName) ->
    TNameStr = utils:to_list(TName),
    NameStr = lists:concat([?INDEX_PREFIX, TNameStr]),
    list_to_atom(NameStr).

%% Builds the name of a secondary index
s_name(TName, IndexName) ->
    TNameStr = utils:to_list(TName),
    INameStr = utils:to_list(IndexName),
    NameStr = lists:concat([?SINDEX_PREFIX, TNameStr, ".", INameStr]),
    list_to_atom(NameStr).

p_put([], _Table) -> [];
p_put(RecordData, Table) when is_list(RecordData) ->
    TName = table:name(Table),
    [?T_COL(PkColName, Type, _)] = column:s_primary_key(Table),
    PKVal = element:get(PkColName, types:to_crdt(Type, ignore), RecordData, Table),
    StateVal = element:get('#st', antidote_crdt_register_mv, RecordData, Table),
    VersionVal = element:get('#version', antidote_crdt_register_lww, RecordData, Table),
    RefsVal = lists:map(fun(?T_FK(FKName, FKType, _, _, _) = Fk) ->
        FKVal = element:get(FKName, types:to_crdt(FKType, ignore), RecordData, Table),
        ParentVersion = element:get_by_name('#version', RecordData),
        ?T_INDEX_REF(FKName, Fk, FKVal, ParentVersion)
    end, table:shadow_columns(Table)),

    ObjBoundKey = {utils:to_atom(PKVal), antidote_crdt_map_go, TName},
    IndexBoundObject = crdt:create_bound_object(p_name(TName), ?INDEX_CRDT, ?METADATA_BUCKET),

    AssignKey = ?BOBJ_FIELD(crdt:assign_lww(ObjBoundKey)),
    AssignState = ?STATE_FIELD(crdt:assign_lww(StateVal)),
    AssignVersion = ?VERSION_FIELD(crdt:assign_lww(VersionVal)),
    AssignRefs = ?REFS_FIELD(lists:map(fun({FKName, FKVal}) ->
        {FKName, crdt:assign_lww(FKVal)}
    end, RefsVal)),

    FullUpdate = [AssignKey, AssignState, AssignVersion, AssignRefs],
    crdt:map_update(IndexBoundObject, {ObjBoundKey, FullUpdate}).

p_put(Key, Table, TxId) ->
    {ok, [Data]} = antidote:read_objects(Key, TxId),
    ok = antidote:update_objects(index:p_put(Data, Table), TxId).

lookup_index(IndexName, Table) ->
    Indexes = table:indexes(Table),
    IndexNameAtom = utils:to_atom(IndexName),
    lists:keyfind(IndexNameAtom, 1, Indexes).

tag_name(TName, Column) ->
    {TName, Column}.

tag_key(TName, Column) ->
    Key = tag_name(TName, Column),
    crdt:create_bound_object(Key, ?ITAG_CRDT, ?METADATA_BUCKET).

tag_subkey(CName) ->
    ?MAP_KEY(CName, ?ITAG_KEY_CRDT).

tag(TName, Column, Value, ITag) ->
    BoundObject = tag_key(TName, Column),
    MapOp = crdt:assign_lww(ITag),
    crdt:single_map_update(BoundObject, Value, ?ITAG_KEY_CRDT, MapOp).

tag(TName, Column, Value, ITag, TxId) ->
    antidote:update_objects(tag(TName, Column, Value, ITag), TxId).

tag_read(TName, CName, Value, TxId) ->
    Key = tag_key(TName, CName),
    {ok, [Map]} = antidote:read_objects(Key, TxId),
    SubKey = tag_subkey(Value),
    proplists:get_value(SubKey, Map).

entry_key(?T_INDEX_ENTRY(Key, _, _, _, _)) -> Key.

entry_bobj(?T_INDEX_ENTRY(_, BObj, _, _, _)) -> BObj.

entry_state(?T_INDEX_ENTRY(_, _, State, _, _)) -> State.

entry_version(?T_INDEX_ENTRY(_, _, _, Version, _)) -> Version.

entry_refs(?T_INDEX_ENTRY(_, _, _, _, Refs)) -> Refs.

get_ref_by_name(RefName, ?T_INDEX_ENTRY(_, _, _, _, Refs)) ->
    Aux = lists:dropwhile(fun({FkName, _}) ->
        FkName /= RefName
    end, Refs),
    case Aux of
        [] -> undefined;
        [FKey | _Rest] -> FKey
    end.

get_ref_by_spec(RefSpec, ?T_INDEX_ENTRY(_, _, _, _, Refs)) ->
    Aux = lists:dropwhile(fun({FkSpec, _}) ->
        FkSpec /= RefSpec
    end, Refs),
    case Aux of
        [] -> undefined;
        [FKey | _Rest] -> FKey
    end.

format_refs(?T_INDEX_ENTRY(_, _, _, _, Refs)) ->
    lists:map(fun(?T_INDEX_REF(_FkName, FkSpec, FkValue, _FkVersion)) ->
        {FkSpec, FkValue}
    end, Refs).

%% ====================================================================
%% Private functions
%% ====================================================================

%has_index(Indexes, IndexName) ->
%  lists:keymember(IndexName, 1, Indexes).

check_keys(Table, Cols) ->
    TCols = column:s_names(Table),
    lists:foldl(fun(Col, Acc) ->
        case lists:member(Col, TCols) of
            false ->
                lists:append(Acc, [Col]);
            true ->
                Acc
        end
    end, [], Cols).

set_table_index(Idx, ?T_TABLE(Name, Policy, Cols, SCols, _Idx)) ->
    ?T_TABLE(Name, Policy, Cols, SCols, Idx).

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

name_test() ->
    Expected = '#_Test',
    ?assertEqual(Expected, p_name("Test")),
    ?assertEqual(Expected, p_name('Test')).

%%put_test() ->
%%  BoundObject = crdt:create_bound_object(key, map, test),
%%  Expected = crdt:add_all({'#_test', ?INDEX_CRDT, ?METADATA_BUCKET}, key),
%%  ?assertEqual(Expected, index:put(BoundObject)).

tag_name_test() ->
    ?assertEqual({test, id}, tag_name(test, id)).

tag_test() ->
    BoundObject = crdt:create_bound_object({test, id}, ?ITAG_CRDT, ?METADATA_BUCKET),
    ExpectedOp = crdt:assign_lww(ipa:touch_cascade()),
    Expected = crdt:single_map_update(BoundObject, "Sam", ?ITAG_KEY_CRDT, ExpectedOp),
    ?assertEqual(Expected, tag(test, id, "Sam", ipa:touch_cascade())).

-endif.

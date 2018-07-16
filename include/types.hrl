
-define(T_TABLE(Name, Policy, Cols, FKeys, Indexes), {Name, Policy, Cols, FKeys, Indexes}).
-define(is_table(Table), is_tuple(Table) andalso tuple_size(Table) =:= 5).

-define(T_ELEMENT(BObj, Table, Ops, Data), {BObj, Table, Ops, Data}).
-define(is_element(Element), is_tuple(Element) andalso tuple_size(Element) =:= 4).

-define(T_COL(Name, Type, Constraint), {Name, Type, Constraint}).
-define(is_column(Column), is_tuple(Column) andalso tuple_size(Column) =:= 3).

-define(T_FK(SName, SType, TTName, TName, OnDelete), {{SName, SType}, {TTName, TName}, OnDelete}).
-define(is_fk(Fk), is_tuple(Fk) andalso tuple_size(Fk) =:= 3).

-define(T_CRP(TableLevel, DepLevel, PDepLevel), {TableLevel, DepLevel, PDepLevel}).
-define(is_crp(Crp), is_tuple(Crp) andalso tuple_size(Crp) =:= 3).

-define(T_INDEX(Name, TName, Cols), {Name, TName, Cols}).
-define(is_index(Index), is_tuple(Index) andalso tuple_size(Index) =:= 3).

-define(T_FILTER(Type, Content), {Type, Content}).
-define(is_filter(Filter), is_tuple(Filter) andalso tuple_size(Filter) =:= 2).

-define(T_INDEX_ENTRY(Key, BoundObject, State, Version, Refs), {Key, {BoundObject, State, Version, Refs}}).
-define(is_index_entry(IndexEntry), is_tuple(IndexEntry) andalso tuple_size(IndexEntry) =:= 2).

-define(T_INDEX_REF(ColName, RefSpec, RefValue, RefVersion), {ColName, {RefSpec, {RefValue, RefVersion}}}).
-define(is_index_ref(IndexRef), is_tuple(IndexRef) andalso tuple_size(IndexRef) =:= 2).
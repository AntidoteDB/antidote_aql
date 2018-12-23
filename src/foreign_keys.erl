
-module(foreign_keys).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("aql.hrl").
-include("parser.hrl").
-include("types.hrl").

-export([from_table/1,
			from_columns/1,
			to_cname/1,
			filter_restrict/1,
			filter_cascade/1]).

from_table(Table) ->
	from_columns(table:columns(Table)).

from_column(Column) ->
	Name = column:name(Column),
	Type = column:type(Column),
	Constraint = column:constraint(Column),
	?FOREIGN_KEY({TName, Attr, DeleteRule}) = Constraint,
	?T_FK(Name, Type, TName, Attr, DeleteRule).

from_columns(Columns) ->
	Fks = maps:filter(fun (_CName, Col) ->
		column:is_foreign_key(Col)
	end, Columns),
	FkList = maps:to_list(Fks),
	lists:map(fun ({_Name, Column}) ->
		from_column(Column)
	end, FkList).

to_cname([{_TName, CName}]) -> CName;
to_cname(ShCName) -> ShCName.

filter_restrict(Columns) ->
	Restrict = maps:filter(fun(_CName, Col) ->
		column:is_restrict_fk(Col)
	end, Columns),
	RestrictList = maps:to_list(Restrict),
	lists:map(fun({_Name, Column}) ->
		from_column(Column)
	end, RestrictList).

filter_cascade(Columns) ->
	Cascade = maps:filter(fun(_CName, Col) ->
		column:is_cascade_fk(Col)
	end, Columns),
	CascadeList = maps:to_list(Cascade),
	lists:map(fun({_Name, Column}) ->
		from_column(Column)
	end, CascadeList).

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

-endif.

%% @author Joao
%% @author Pedro Lopes
%% @doc @todo Add description to select.

-module(select).

-include("parser.hrl").
-include("aql.hrl").
-include("types.hrl").

-define(CONDITION(FieldName, Comparator, Value), {FieldName, Comparator, Value}).
-define(CONJUNCTION, ?CONJUNCTIVE_KEY("AND")).
-define(DISJUNCTION, ?DISJUNCTIVE_KEY("OR")).
-define(FUNCTION(Name, Args), {func, Name, Args}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ====================================================================
%% API functions
%% ====================================================================
-export([exec/3]).

-export([table/1,
				projection/1,
				where/1]).

exec({Table, _Tables}, Select, TxId) ->
	%TName = table:name(Table),
	Cols = table:columns(Table),
	Projection = projection(Select),
	% TODO validate projection fields
	Condition = where(Select),
	NewCondition = send_offset(Condition, Cols, []),
	Filter = prepare_filter(Table, Projection, NewCondition),
	case antidote:query_objects(Filter, TxId) of
		{ok, Result} ->
			FinalResult = apply_offset(Result, Cols, []),
			{ok, FinalResult};
		{error, _} = ErrorMsg ->
			ErrorMsg
	end.
	%Keys = where:scan(TName, Condition, TxId),
	%case Keys of
	%	[] -> {ok, []};
	%	_Else ->
	%		{ok, Results} = antidote:read_objects(Keys, TxId),
	%		VisibleResults = filter_visible(Results, Table, TxId),
	%		ProjectionResult = project(Projection, VisibleResults, [], Cols),
	%		ActualRes = apply_offset(ProjectionResult, Cols, []),
	%		{ok, ActualRes}
	%end.

table({TName, _Projection, _Where}) -> TName.

projection({_TName, Projection, _Where}) -> Projection.

where({_TName, _Projection, Where}) -> Where.

%% ====================================================================
%% Private functions
%% ====================================================================
send_offset(?PARSER_WILDCARD, _Cols, Acc) -> Acc;
send_offset([{disjunctive, _} = Cond | Conds], Cols, Acc) ->
	send_offset(Conds, Cols, lists:append(Acc, [Cond]));
send_offset([{conjunctive, _} = Cond | Conds], Cols, Acc) ->
	send_offset(Conds, Cols, lists:append(Acc, [Cond]));
send_offset([Condition | Conds], Cols, Acc) when is_list(Condition) ->
	NewAcc = send_offset(Condition, Cols, []),
	send_offset(Conds, Cols, lists:append(Acc, [NewAcc]));
send_offset([Condition | Conds], Cols, Acc) ->
	?CONDITION(FieldName, Comparator, Value) = Condition,
	Column = maps:get(FieldName, Cols),
	Constraint = column:constraint(Column),
	Type = column:type(Column),
	case {Type, Constraint} of
		{?AQL_COUNTER_INT, ?CHECK_KEY({_Key, ?COMPARATOR_KEY(Comp), Offset})} ->
			InvertComp = case Comp of
										 ?PARSER_LESSER -> invert_comparator(Comparator);
										 ?PARSER_LEQ -> invert_comparator(Comparator);
										 _ -> Comparator
									 end,
			AQLCounterValue = bcounter:to_bcounter(Value, Offset, Comp),
			NewCond = ?CONDITION(FieldName, InvertComp, AQLCounterValue),
			send_offset(Conds, Cols, lists:append(Acc, [NewCond]));
		_Else ->
			send_offset(Conds, Cols, lists:append(Acc, [Condition]))
	end;
send_offset([], _Cols, Acc) -> Acc.

invert_comparator(?PARSER_GREATER) -> ?PARSER_LESSER;
invert_comparator(?PARSER_LESSER) -> ?PARSER_GREATER;
invert_comparator(?PARSER_GEQ) -> ?PARSER_LEQ;
invert_comparator(?PARSER_LEQ) -> ?PARSER_GEQ;
invert_comparator(Comp) -> Comp.

prepare_filter(Table, Projection, Conditions) ->
	VisibilityConds = build_visibility_conditions(Table),
	NewConditions = case Conditions of
										[] -> VisibilityConds;
										Conditions -> lists:append([[Conditions], [?CONJUNCTION], [VisibilityConds]])
									end,

	Conjunctions = group_conjunctions(NewConditions),

	TableName = table:name(Table),
	TablesField = ?T_FILTER(tables, [TableName]),
	ProjectionField = ?T_FILTER(projection, Projection),

	ConditionsField = ?T_FILTER(conditions, Conjunctions),
	[TablesField, ProjectionField, ConditionsField].

%% The idea is to build additional conditions that concern visibility.
%% Those conditions are then sent to the Antidote node.
%% Former Form: ((#st = i OR #st = t) AND fk_col1 <> dc AND fk_col2 <> dc AND ...)
%% New Form:
%% - Update-wins:
%% 			(state(row.pk) <> d AND
%% 			 state(row.fk_col1) <> d AND
%% 			 state(row.fk_col2) <> d ...)
%% - Delete-wins:
%% 			(state(row.pk) <> d AND
%% 			 (assert_version(row.fk_col1) = true AND state(row.fk_col1) <> d) AND
%% 			 (assert_version(row.fk_col2) = true AND state(row.fk_col2) <> d) ...)
build_visibility_conditions(Table) ->
  Policy = table:policy(Table),
  Rule = crp:get_rule(Policy),
	ExplicitConds = [explicit_state_conds(Rule)],
	ImplicitConds = implicit_state_conds(Table, Rule),
	case ImplicitConds of
		[] -> ExplicitConds;
		_Else -> lists:append([ExplicitConds, [?CONJUNCTION], ImplicitConds])
	end.

explicit_state_conds(Rule) ->
  Func = ?FUNCTION(find_last, ['#st', Rule]),
	%ICond = {Func, ?PARSER_EQUALITY, i},
	%TCond = {Func, ?PARSER_EQUALITY, t},
	%[ICond, ?DISJUNCTION, TCond].
	[{Func, ?PARSER_NEQ, d}].

implicit_state_conds(Table, Rule) ->
	ShCols = lists:filter(fun(?T_FK(FkName, _, _, _, _)) ->
		length(FkName) == 1
	end, table:shadow_columns(Table)),
	implicit_state_conds(ShCols, Rule, []).

implicit_state_conds([?T_FK(FkName, _, FkTable, _, _) | []], _Rule, Acc) ->
  %Func = ?FUNCTION(find_first, [FkName, Rule]),
	%lists:append(Acc, [{Func, ?PARSER_NEQ, dc}]);
	Func = ?FUNCTION(assert_version, [FkName, FkTable]),
	lists:append(Acc, [{Func, ?PARSER_EQUALITY, true}]);
implicit_state_conds([?T_FK(FkName, _, FkTable, _, _) | Tail], Rule, Acc) ->
  %Func = ?FUNCTION(find_first, [FkName, Rule]),
	%NewAcc = lists:append(Acc, [{Func, ?PARSER_NEQ, dc}, ?CONJUNCTION]),
	Func = ?FUNCTION(assert_version, [FkName, FkTable]),
	NewAcc = lists:append(Acc, [{Func, ?PARSER_EQUALITY, true}, ?CONJUNCTION]),
	implicit_state_conds(Tail, Rule, NewAcc);
implicit_state_conds([], _Rule, Acc) -> Acc.

filter_visible(Results, Table, TxId) ->
	filter_visible(Results, Table, TxId, []).

filter_visible([Result | Results], Table, TxId, Acc) ->
	case element:is_visible(Result, Table, TxId) of
		  true -> filter_visible(Results, Table, TxId, lists:append(Acc, [Result]));
			_Else -> filter_visible(Results, Table, TxId, Acc)
	end;
filter_visible([], _Table, _TxId, Acc) ->
	Acc.

% groups of elements
apply_offset([Result | Results], Cols, Acc) when is_list(Result) ->
	Result1 = apply_offset(Result, Cols, []),
	apply_offset(Results, Cols, Acc ++ [Result1]);
% groups of columns
apply_offset([{{'#st', _T}, _} | Values], Cols, Acc) ->
	apply_offset(Values, Cols, Acc);
apply_offset([{{Key, Type}, V} | Values], Cols, Acc) ->
  Col = maps:get(Key, Cols),
  Cons = column:constraint(Col),
	case {Type, Cons} of
    {?CRDT_BCOUNTER_INT, ?CHECK_KEY({_Key, ?COMPARATOR_KEY(Comp), Offset})} ->
			AQLCounterValue = bcounter:from_bcounter(Comp, V, Offset),
			NewAcc = lists:append(Acc, [{Key, AQLCounterValue}]),
      apply_offset(Values, Cols, NewAcc);
    _Else ->
			NewAcc = lists:append(Acc, [{Key, V}]),
			apply_offset(Values, Cols, NewAcc)
  end;
apply_offset([], _Cols, Acc) -> Acc.


project(Projection, [[{{'#st', _T}, _V}] | Results], Acc, Cols) ->
	project(Projection, Results, Acc, Cols);
project(Projection, [[] | Results], Acc, Cols) ->
	project(Projection, Results, Acc, Cols);
project(Projection, [Result | Results], Acc, Cols) ->
	ProjRes = project_row(Projection, Result, [], Cols),
	project(Projection, Results, Acc ++ [ProjRes], Cols);
project(_Projection, [], Acc, _Cols) ->
	Acc.

% if key is list (i.e. shadow col), ignore
project_row(Projection, [{{Key, _T}, _V} | Data], Acc, Cols) when is_list(Key) ->
	project_row(Projection, Data, Acc, Cols);
% if wildcard, accumulate
project_row(?PARSER_WILDCARD, [ColData | Data], Acc, Cols) ->
	project_row(?PARSER_WILDCARD, Data, Acc ++ [ColData], Cols);
% if wildcard and no more data to project, return data accumulated
project_row(?PARSER_WILDCARD, [], Acc, _Cols) ->
	Acc;
project_row([ColName | Tail], Result, Acc, Cols) ->
	{{Key, _Type}, Value} = get_value(ColName, Result),
	Col = column:s_get(Cols, Key),
	Type = column:type(Col),
	NewResult = proplists:delete(ColName, Result),
	NewAcc = Acc ++ [{{Key, Type}, Value}],
	project_row(Tail, NewResult, NewAcc, Cols);
project_row([], _Result, Acc, _Cols) ->
	Acc.

get_value(Key, [{{Name, _Type}, _Value} = H| T]) ->
	case Key of
		Name ->
			H;
		_Else ->
			get_value(Key, T)
	end;
get_value(_Key, []) ->
	undefined.

group_conjunctions(?PARSER_WILDCARD) ->
  [];
group_conjunctions(WhereClause) when is_list(WhereClause) ->
	BoolConnectors = lists:filter(fun(Elem) ->
			case Elem of
				{Type, _} when (Type == disjunctive) or (Type == conjunctive)
					-> true;
				_Else -> false
			end
		end, WhereClause),
  FilterClause = lists:filter(fun(Elem) ->
		case Elem of
			{_Type, _} -> false;
			_Else -> true
		end
  end, WhereClause),
	[First | Tail] = FilterClause,
	case is_list(First) of
		true -> group_conjunctions(Tail, BoolConnectors, [{sub, group_conjunctions(First)}], []);
		false -> group_conjunctions(Tail, BoolConnectors, [First], [])
	end.

group_conjunctions([Comp | Tail], [{conjunctive, _} | Tail2], Curr, Final) ->
	Conj = case is_list(Comp) of
					 true -> {sub, group_conjunctions(Comp)};
					 false -> Comp
				 end,
	group_conjunctions(Tail, Tail2, lists:append(Curr, [Conj]), Final);
group_conjunctions([Comp | Tail], [{disjunctive, _} | Tail2], Curr, Final) ->
	Conj = case is_list(Comp) of
					 true -> {sub, group_conjunctions(Comp)};
					 false -> Comp
				 end,
	group_conjunctions(Tail, Tail2, [Conj], lists:append(Final, [Curr]));
group_conjunctions([], [], Curr, Final) ->
	lists:append(Final, [Curr]).

%%====================================================================
%% Eunit tests
%%====================================================================

-ifdef(TEST).

conjunction_test() ->
  DefaultComp = {attr, [{equality, ignore}], val},
  TestClause1 = [
    DefaultComp,
    {conjunctive, ignore},
    DefaultComp,
    {disjunctive, ignore},
    DefaultComp
	],
  TestClause2 = [
    DefaultComp,
    {disjunctive, ignore},
    DefaultComp,
    {disjunctive, ignore},
    DefaultComp
  ],
  TestClause3 = [
    DefaultComp,
    {conjunctive, ignore},
    DefaultComp,
    {conjunctive, ignore},
    DefaultComp
  ],
  Res1 = group_conjunctions(TestClause1),
  Res2 = group_conjunctions(TestClause2),
  Res3 = group_conjunctions(TestClause3),
  ?assertEqual(Res1, [[DefaultComp, DefaultComp], [DefaultComp]]),
  ?assertEqual(Res2, [[DefaultComp], [DefaultComp], [DefaultComp]]),
  ?assertEqual(Res3, [[DefaultComp, DefaultComp, DefaultComp]]).

conjunction_parenthesis_test() ->
	DefaultComp = {attr, [{equality, ignore}], val},
	TestClause1 = [
		[DefaultComp,
		{conjunctive, ignore},
		DefaultComp,
		{disjunctive, ignore},
		DefaultComp],
		{conjunctive, ignore},
		DefaultComp
	],
	Res1 = group_conjunctions(TestClause1),
	?assertEqual([[{sub, [[DefaultComp, DefaultComp], [DefaultComp]]}, DefaultComp]], Res1).

-endif.
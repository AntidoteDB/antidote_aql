%%====================================================================
%% Definitions
%%====================================================================
Definitions.

WhiteSpace = ([\000-\s]|%.*)
Equality = =
NotEquality = (<>)
Greater = >
Lesser = <
GreaterEq = (>=)
LesserEq = (<=)
Plus = \+
Minus = -
CharValues = [A-Za-z]
WildCard = \*
IntegerValues = [0-9]
StartList = \(
EndList = \)
Sep = ,
SemiColon = ;
String = '([^']*?)'

%%====================================================================
%% Rules
%%====================================================================
Rules.

% show related tokens
(show|SHOW) : {token, ?SHOW_CLAUSE(TokenChars)}.
(tables|TABLES) : {token, ?TABLES_CLAUSE(TokenChars)}.

% index related tokens
(index|INDEX) : {token, ?INDEX_CLAUSE(TokenChars)}.
(indexes|INDEXES) : {token, ?INDEXES_CLAUSE(TokenChars)}.
(on|ON) : {token, ?ON_KEY(TokenChars)}.

% select query related tokens
(select|SELECT) : {token, ?SELECT_CLAUSE(TokenChars)}.
(from|FROM) : {token, ?FROM_CLAUSE(TokenChars)}.

% where clause related tokens
(where|WHERE) : {token, ?WHERE_CLAUSE(TokenChars)}.
(and|AND) : {token, ?CONJUNCTIVE_KEY(TokenChars)}.
(or|OR) : {token, ?DISJUNCTIVE_KEY(TokenChars)}.

% insert query related tokens
(insert|INSERT) : {token, ?INSERT_CLAUSE(TokenChars)}.
(into|INTO) : {token, ?INTO_KEY(TokenChars)}.

% create query related tokens
(create|CREATE) : {token, ?CREATE_CLAUSE(TokenChars)}.
(table|TABLE) : {token, ?TABLE_KEY(TokenChars)}.
(values|VALUES) : {token, ?VALUES_CLAUSE(TokenChars)}.
(partition|PARTITION) : {token, ?PARTITION_CLAUSE(TokenChars)}.

% delete query related tokens
(delete|DELETE) : {token, ?DELETE_CLAUSE(TokenChars)}.

% transaction related tokens
(begin|BEGIN) : {token, ?BEGIN_CLAUSE(TokenChars)}.
(commit|COMMIT) : {token, ?COMMIT_CLAUSE(TokenChars)}.
(rollback|ROLLBACK) : {token, ?ROLLBACK_CLAUSE(TokenChars)}.
(transaction|TRANSACTION) : {token, ?TRANSACTION_KEY(TokenChars)}.

% quit program related token
(quit|QUIT) : {token, ?QUIT_CLAUSE(TokenChars)}.

% conflict resolution policies
(update-wins|UPDATE-WINS) : {token, ?CRP_KEY(?ADD_WINS)}.
(delete-wins|DELETE-WINS) : {token, ?CRP_KEY(?REMOVE_WINS)}.

% update query related tokens
(update|UPDATE) : {token, ?UPDATE_CLAUSE(TokenChars)}.
(set|SET) : {token, ?SET_CLAUSE(TokenChars)}.

% set operations
(assign|ASSIGN) : {token, ?ASSIGN_OP(TokenChars)}.
(increment|INCREMENT) : {token, ?INCREMENT_OP(TokenChars)}.
(decrement|DECREMENT) : {token, ?DECREMENT_OP(TokenChars)}.

% constraints
(primary|PRIMARY) : {token, ?PRIMARY_KEY(TokenChars)}.
(foreign|FOREIGN) : {token, ?FOREIGN_KEY(TokenChars)}.
(key|KEY) : {token, ?KEY_KEY(TokenChars)}.
(references|REFERENCES) : {token, ?REFERENCES_KEY(TokenChars)}.
(check|CHECK) : {token, ?CHECK_KEY(TokenChars)}.
(cascade|CASCADE) : {token, ?CASCADE_CLAUSE(TokenChars)}.

% default
(default|DEFAULT) : {token, ?DEFAULT_KEY(TokenChars)}.

% attribute types
(varchar|VARCHAR) : {token, ?ATTR_KEY(?AQL_VARCHAR)}.
(boolean|BOOLEAN) : {token, ?ATTR_KEY(?AQL_BOOLEAN)}.
(int|INT|integer|INTEGER) : {token, ?ATTR_KEY(?AQL_INTEGER)}.
(counter_int|COUNTER_INT) : {token, ?ATTR_KEY(?AQL_COUNTER_INT)}.

% boolean atoms
(false|FALSE) : {token, ?PARSER_BOOLEAN(false)}.
(true|TRUE) : {token, ?PARSER_BOOLEAN(true)}.


{CharValues}+ : A = list_to_atom(TokenChars),
				{token, ?PARSER_ATOM(A)}.

{String} : S = strip_value(TokenChars),
			{token, ?PARSER_STRING(S)}.

{IntegerValues}+ : {N, _} = string:to_integer(TokenChars),
				{token, ?PARSER_NUMBER(N)}.

{Equality} : {token, ?PARSER_EQUALITY}.
{NotEquality} : {token, ?PARSER_NEQ}.
{Greater} : {token, ?PARSER_GREATER}.
{Lesser} : {token, ?PARSER_LESSER}.
{GreaterEq} : {token, ?PARSER_GEQ}.
{LesserEq} : {token, ?PARSER_LEQ}.
{Plus} : {token, ?PARSER_PLUS}.
{Minus} : {token, ?PARSER_MINUS}.
{WildCard} : {token, ?PARSER_WILDCARD}.
{WhiteSpace}+ : skip_token.

{StartList} : {token, ?PARSER_SLIST}.
{EndList} : {token, ?PARSER_ELIST}.
{Sep} : {token, ?PARSER_SEP}.
{SemiColon} : {token, ?PARSER_SCOLON}.

%%====================================================================
%% Erlang Code
%%====================================================================
Erlang code.

-include("parser.hrl").
-include("aql.hrl").

strip_value(Value) ->
	string:strip(Value, both, $').

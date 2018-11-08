%%====================================================================
%% Nonterminals
%%====================================================================
Nonterminals
query
statement
admin
%show
show_query
%select
select_query projection select_fields
%where
where_clauses where_clause comparison
%insert
insert_query insert_keys_clause insert_keys insert_values_clause insert_values
%delete
delete_query
%create
create_query create_keys attribute attribute_constraint create_index_keys check_comparator
attribute_name
%update
update_query set_clause set_assignments set_assignment
%txs
begin_transaction commit_transaction rollback_transaction
%utils
value atom number_unwrap
%quit program
quit_program
.

%%====================================================================
%% Terminals
%%====================================================================
Terminals
%show
show tables
%index
index indexes on
%select
select wildcard from
%where
where
%comparison
equality notequality greater lesser greatereq lessereq
%insert
insert into values
%delete
delete
%create
create table partition crp primary foreign key references default check
attribute_type cascade
%update
update set
%tx
begin commit rollback transaction
%types
atom_value string number boolean
%expression
plus minus conjunctive disjunctive
%list
sep start_list end_list semi_colon
%quit
quit
.

%%====================================================================
%% Rootsymbol
%%====================================================================
Rootsymbol query.

%%====================================================================
%% Rules
%%====================================================================

query -> statement : '$1'.

statement -> statement semi_colon statement : lists:append('$1', '$3').

statement -> statement semi_colon :	'$1'.

statement -> admin : '$1'.

statement -> select_query : ['$1'].

statement -> insert_query : ['$1'].

statement -> delete_query : ['$1'].

statement -> update_query :	['$1'].

statement -> create_query :	['$1'].

statement -> begin_transaction : ['$1'].

statement -> commit_transaction : ['$1'].

statement -> rollback_transaction : ['$1'].

statement -> quit_program : ['$1'].

comparison -> equality : '$1'.
comparison -> notequality : '$1'.
comparison -> greater : '$1'.
comparison -> lesser : '$1'.
comparison -> greatereq : '$1'.
comparison -> lessereq : '$1'.

admin -> show_query : ['$1'].

%%--------------------------------------------------------------------
%% show
%%--------------------------------------------------------------------
show_query ->
	show index from atom :
	?SHOW_CLAUSE({?INDEX_TOKEN, '$4'}).

show_query ->
    show index atom from atom :
    ?SHOW_CLAUSE({?INDEX_TOKEN, '$3', '$5'}).

show_query ->
    show indexes from atom :
    ?SHOW_CLAUSE({?INDEXES_TOKEN, '$4'}).

show_query ->
	show tables :
	?SHOW_CLAUSE(?TABLES_TOKEN).

%%--------------------------------------------------------------------
%% select query
%%--------------------------------------------------------------------
select_query ->
    select projection from atom :
		?SELECT_CLAUSE({'$4', '$2', ?PARSER_WILDCARD}).

select_query ->
    select projection from atom where where_clauses:
		?SELECT_CLAUSE({'$4', '$2', '$6'}).

projection ->
    wildcard :
    '$1'.

projection ->
	select_fields:
	'$1'.

select_fields ->
  select_fields sep atom :
	lists:append('$1', ['$3']).

select_fields ->
	atom :
	['$1'].

%%--------------------------------------------------------------------
%% where clause
%%--------------------------------------------------------------------

where_clauses ->
    where_clauses conjunctive where_clauses :
    lists:append(['$1', ['$2'], '$3']).

where_clauses ->
    where_clauses disjunctive where_clauses :
    lists:append(['$1', ['$2'], '$3']).

where_clauses ->
    start_list where_clauses conjunctive where_clauses end_list :
    [lists:append(['$2', ['$3'], '$4'])].

where_clauses ->
    start_list where_clauses disjunctive where_clauses end_list :
    [lists:append(['$2', ['$3'], '$4'])].

where_clauses ->
    where_clause :
	['$1'].

% Uncomment to support this
%where_clauses ->
%    start_list where_clause end_list :
%    ['$2'].

where_clause ->
	atom comparison value :
	{'$1', '$2', '$3'}.

%%--------------------------------------------------------------------
%% insert query
%%--------------------------------------------------------------------
insert_query ->
    insert into atom insert_keys_clause
    values insert_values_clause :
		?INSERT_CLAUSE({'$3', '$4', '$6'}).

insert_query ->
    insert into atom
    values insert_values_clause :
		?INSERT_CLAUSE({'$3', ?PARSER_WILDCARD, '$5'}).

insert_keys_clause ->
    start_list insert_keys end_list :
    '$2'.

insert_keys ->
	insert_keys sep atom :
	lists:append('$1', ['$3']).

insert_keys ->
	atom :
	['$1'].

insert_values_clause ->
  start_list insert_values end_list :
  '$2'.

insert_values ->
	insert_values sep value :
	lists:append('$1', ['$3']).

insert_values ->
	value :
	['$1'].

%%--------------------------------------------------------------------
%% update query
%%--------------------------------------------------------------------

update_query ->
	update atom set_clause :
	?UPDATE_CLAUSE({'$2', '$3', ?PARSER_WILDCARD}).

update_query ->
	update atom set_clause where where_clauses :
	?UPDATE_CLAUSE({'$2', '$3', '$5'}).

set_clause ->
	set set_assignments :
	?SET_CLAUSE('$2').

set_assignments ->
	set_assignments conjunctive set_assignment :
	lists:append('$1', ['$3']).

set_assignments ->
	set_assignment :
	['$1'].

%assignment expression
set_assignment ->
	atom equality value :
	{'$1', ?ASSIGN_OP('$2'), '$3'}.

%increment/decrement expression
%set_assignment ->
%	atom increment :
%	atom equality atom plus 1
%	{'$1', ?INCREMENT('$2'), 1}.

set_assignment ->
%	atom increment number_unwrap :
    atom equality atom plus number_unwrap :
	{'$1', ?INCREMENT_OP('$4'), '$5'}.

%set_assignment ->
%	atom decrement :
%	{'$1', '$2', 1}.

set_assignment ->
%	atom decrement number_unwrap :
    atom equality atom minus number_unwrap :
	{'$1', ?DECREMENT_OP('$4'), '$5'}.

%%--------------------------------------------------------------------
%% create query
%%--------------------------------------------------------------------
create_query ->
	create crp table atom start_list create_keys end_list :
	?CREATE_CLAUSE(?T_TABLE('$4', crp:set_table_level(unwrap_type('$2'), crp:new()), '$6', [], [], undefined)).

create_query ->
    create crp table atom start_list create_keys end_list partition on start_list attribute_name end_list :
    ?CREATE_CLAUSE(?T_TABLE('$4', crp:set_table_level(unwrap_type('$2'), crp:new()), '$6', [], [], ['$11'])).

create_query ->
    create index atom on atom start_list create_index_keys end_list :
    ?CREATE_CLAUSE(?T_INDEX('$3', '$5', '$7')).

create_keys ->
	create_keys sep attribute :
	lists:append('$1', ['$3']).

create_keys ->
	attribute :
	['$1'].

attribute ->
	attribute_name attribute_type attribute_constraint :
	?T_COL('$1', unwrap_type('$2'), '$3').

attribute ->
	attribute_name attribute_type :
	?T_COL('$1', unwrap_type('$2'), ?NO_CONSTRAINT).

attribute_constraint ->
	primary key :
	?PRIMARY_TOKEN.

attribute_constraint ->
	foreign key crp references atom start_list atom end_list :
	?FOREIGN_KEY({'$5', '$7', unwrap_type('$3'), ?RESTRICT_TOKEN}).

attribute_constraint ->
    foreign key references atom start_list atom end_list :
    ?FOREIGN_KEY({'$4', '$6', unwrap_type(?NO_CONCURRENCY_KEY), ?RESTRICT_TOKEN}).

attribute_constraint ->
	foreign key crp references atom start_list atom end_list on delete cascade :
	?FOREIGN_KEY({'$5', '$7', unwrap_type('$3'), ?CASCADE_TOKEN}).

attribute_constraint ->
	foreign key references atom start_list atom end_list on delete cascade :
	?FOREIGN_KEY({'$4', '$6', unwrap_type(?NO_CONCURRENCY_KEY), ?CASCADE_TOKEN}).

attribute_constraint ->
	default value :
	?DEFAULT_KEY('$2').

attribute_constraint ->
	check start_list attribute_name check_comparator number_unwrap end_list :
	?CHECK_KEY({'$3', '$4', '$5'}).

attribute_name ->
	atom :
	'$1'.

check_comparator -> greater : ?GREATER_KEY.
check_comparator -> lesser : ?LESSER_KEY.
check_comparator -> greatereq : ?GREATEREQ_KEY.
check_comparator -> lessereq : ?LESSEREQ_KEY.

create_index_keys ->
	create_index_keys sep atom :
	lists:append('$1', ['$3']).

create_index_keys ->
	atom :
	['$1'].

%%--------------------------------------------------------------------
%% delete
%%--------------------------------------------------------------------

delete_query ->
	delete from atom :
	?DELETE_CLAUSE({'$3', ?PARSER_WILDCARD}).

delete_query ->
	delete from atom where where_clauses :
	?DELETE_CLAUSE({'$3', '$5'}).

%%--------------------------------------------------------------------
%% transactions
%%--------------------------------------------------------------------

begin_transaction ->
	begin transaction :
	?BEGIN_CLAUSE(?TRANSACTION_TOKEN).

commit_transaction ->
    commit transaction :
    ?COMMIT_CLAUSE(?TRANSACTION_TOKEN).

rollback_transaction ->
    rollback transaction :
    ?ROLLBACK_CLAUSE(?TRANSACTION_TOKEN).

%%--------------------------------------------------------------------
%% quit program
%%--------------------------------------------------------------------
quit_program ->
    quit :
    ?QUIT_CLAUSE(?QUIT_TOKEN).

%%--------------------------------------------------------------------
%% utils
%%--------------------------------------------------------------------
atom ->
    atom_value :
    unwrap_type('$1').

number_unwrap ->
    number :
    unwrap_type('$1').

value ->
	number :
	unwrap_type('$1').

value ->
	string :
	unwrap_type('$1').

value ->
    boolean :
    unwrap_type('$1').

%%====================================================================
%% Erlang Code
%%====================================================================
Erlang code.

-include("parser.hrl").
-include("types.hrl").

unwrap_type(?PARSER_TYPE(_Type, Value)) -> Value.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%====================================================================
%% Eunit tests
%%====================================================================
-ifdef(TEST).

test_parser(String) ->
	{ok, Tokens, _} = scanner:string(String),
	{ok, _ParserTree} = parse(Tokens).

show_tables_test() ->
	test_parser("SHOW TABLES").

show_index_test() ->
	test_parser("SHOW INDEX FROM TestTable"),
	test_parser("SHOW INDEXES FROM TestTable"),
	test_parser("SHOW INDEX TestIndex FROM TestTable").

create_table_simple_test() ->
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR, b INTEGER)"),
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR)"),
	test_parser("CREATE DELETE-WINS TABLE TestA (a VARCHAR);CREATE DELETE-WINS TABLE TestB (b INTEGER)").

create_table_pk_test() ->
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR PRIMARY KEY, b INTEGER)"),
	test_parser("CREATE UPDATE-WINS TABLE Test (a INTEGER PRIMARY KEY, b INTEGER)"),
	test_parser("CREATE UPDATE-WINS TABLE Test (a BOOLEAN PRIMARY KEY, b INTEGER)").

create_table_def_test() ->
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR, b INTEGER DEFAULT 5)"),
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR, b BOOLEAN DEFAULT false)"),
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR, b VARCHAR DEFAULT 'example')").

create_table_check_test() ->
	test_parser("CREATE UPDATE-WINS TABLE Test(a INTEGER, b COUNTER_INT CHECK (b > 0))"),
	test_parser("CREATE UPDATE-WINS TABLE Test(a INTEGER, b COUNTER_INT CHECK (b >= 0))"),
	test_parser("CREATE UPDATE-WINS TABLE Test(a INTEGER, b COUNTER_INT CHECK (b < 0))"),
	test_parser("CREATE UPDATE-WINS TABLE Test(a INTEGER, b COUNTER_INT CHECK (b <= 0))").

create_table_fk_test() ->
	test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR, b INTEGER FOREIGN KEY UPDATE-WINS REFERENCES TestB(b))"),
	test_parser("CREATE DELETE-WINS TABLE Test (a VARCHAR, b INTEGER FOREIGN KEY REFERENCES TestB(b))").

create_table_fk_cascade_test() ->
    test_parser("CREATE DELETE-WINS TABLE TestA (a VARCHAR, b INTEGER FOREIGN KEY DELETE-WINS REFERENCES TestB(b) ON DELETE CASCADE)"),
    test_parser("CREATE DELETE-WINS TABLE TestA (a VARCHAR, b INTEGER FOREIGN KEY REFERENCES TestB(b) ON DELETE CASCADE)").

create_table_partition_test() ->
    test_parser("CREATE UPDATE-WINS TABLE Test (a VARCHAR, b INTEGER) PARTITION ON (b)").

create_index_simple_test() ->
    test_parser("CREATE INDEX TestIdx ON Table (a)"),
    test_parser("CREATE INDEX TestIdx ON Table (a, b)").

update_simple_test() ->
	test_parser("UPDATE Test SET name = 'aaa'"),
	test_parser("UPDATE Test SET name = 'a'; UPDATE Test SET name = 'b'").

update_multiple_test() ->
	test_parser("UPDATE Test SET name = 'aaa' AND age = age + 3"),
	test_parser("UPDATE Test SET name = 'aaa' AND age = age + 3 AND loc = 'en' WHERE loc = 'pt'").

update_assign_test() ->
	test_parser("UPDATE Test SET name = 'aaa' WHERE name = 'a'"),
	test_parser("UPDATE Test SET age = 50 WHERE name = 'aa'").

update_increment_test() ->
	test_parser("UPDATE Test SET countCars = countCars + 1 WHERE model = 'b'"),
	test_parser("UPDATE Test SET countCars = countCars + 2 WHERE model = 'b'").

update_decrement_test() ->
	test_parser("UPDATE Test SET countCars = countCars - 1 WHERE model = 'b'"),
	test_parser("UPDATE Test SET countCars = countCars - 2 WHERE model = 'b'").

insert_simple_test() ->
	test_parser("INSERT INTO Test VALUES ('a', 5, 'b')"),
	test_parser("INSERT INTO Test (a, b, c) VALUES ('a', 5, 'b')"),
	test_parser("INSERT INTO Test VALUES ('a')"),
	test_parser("INSERT INTO Test (a) VALUES (5)").

delete_simple_test() ->
	test_parser("DELETE FROM Test"),
	test_parser("DELETE FROM Test WHERE id = 5").

select_simple_test() ->
	test_parser("SELECT * FROM Test"),
	test_parser("SELECT a, b FROM Test WHERE c = 5").

select_projection_test() ->
	test_parser("SELECT a FROM Test"),
	test_parser("SELECT a, b FROM Test").

select_where_test() ->
	test_parser("SELECT a FROM Test WHERE b = 2"),
	test_parser("SELECT a FROM Test WHERE b = 2 AND c = 3 AND d = 4"),
	test_parser("SELECT a FROM Test WHERE b >= 2 OR c <= 3 AND d = 4"),
	test_parser("SELECT a FROM Test WHERE b > 2 AND c < 3 OR d <> 4"),
	test_parser("SELECT a FROM Test WHERE b = 2 AND (c <= 3 OR d = 4)"),
	test_parser("SELECT a FROM Test WHERE (b >= 2 AND c = 3 OR d <> 4)"),
	test_parser("SELECT a FROM Test WHERE (b <> 2 AND c < 3) OR d > 4").

transaction_test() ->
    test_parser("BEGIN TRANSACTION"),
    test_parser("COMMIT TRANSACTION"),
    test_parser("ROLLBACK TRANSACTION").

quit_test() ->
    test_parser("QUIT"),
    test_parser("quit").

-endif.

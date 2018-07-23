-module(parser).
-export([parse/1, parse_and_scan/1, format_error/1]).
-file("src/parser.yrl", 401).

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
	test_parser("CREATE AW TABLE Test (a VARCHAR, b INTEGER)"),
	test_parser("CREATE AW TABLE Test (a VARCHAR)"),
	test_parser("CREATE AW TABLE TestA (a VARCHAR);CREATE AW TABLE TestB (b INTEGER)").

create_table_pk_test() ->
	test_parser("CREATE AW TABLE Test (a VARCHAR PRIMARY KEY, b INTEGER)"),
	test_parser("CREATE AW TABLE Test (a INTEGER PRIMARY KEY, b INTEGER)"),
	test_parser("CREATE AW TABLE Test (a BOOLEAN PRIMARY KEY, b INTEGER)").

create_table_def_test() ->
	test_parser("CREATE AW TABLE Test (a VARCHAR, b INTEGER DEFAULT 5)"),
	test_parser("CREATE AW TABLE Test (a VARCHAR, b BOOLEAN DEFAULT false)"),
	test_parser("CREATE AW TABLE Test (a VARCHAR, b VARCHAR DEFAULT 'example')").

create_table_check_test() ->
	test_parser("CREATE AW TABLE Test(a INTEGER, b COUNTER_INT CHECK (b > 0))"),
	test_parser("CREATE AW TABLE Test(a INTEGER, b COUNTER_INT CHECK (b >= 0))"),
	test_parser("CREATE AW TABLE Test(a INTEGER, b COUNTER_INT CHECK (b < 0))"),
	test_parser("CREATE AW TABLE Test(a INTEGER, b COUNTER_INT CHECK (b <= 0))").

create_table_fk_test() ->
	test_parser("CREATE AW TABLE Test (a VARCHAR, b INTEGER FOREIGN KEY UPDATE-WINS REFERENCES TestB(b))").

create_table_fk_cascade_test() ->
    test_parser("CREATE RW TABLE TestA (a VARCHAR, b INTEGER FOREIGN KEY DELETE-WINS REFERENCES TestB(b) ON DELETE CASCADE)").

create_table_partition_test() ->
    test_parser("CREATE AW TABLE Test (a VARCHAR, b INTEGER) PARTITION ON (b)").

create_index_simple_test() ->
    test_parser("CREATE INDEX TestIdx ON Table (a)"),
    test_parser("CREATE INDEX TestIdx ON Table (a, b)").

update_simple_test() ->
	test_parser("UPDATE Test SET name ASSIGN 'aaa'"),
	test_parser("UPDATE Test SET name ASSIGN 'a';UPDATE Test SET name ASSIGN 'b'").

update_multiple_test() ->
	test_parser("UPDATE Test SET name ASSIGN 'aaa' AND age INCREMENT 3"),
	test_parser("UPDATE Test SET name ASSIGN 'aaa' AND age INCREMENT 3 AND loc ASSIGN 'en' WHERE loc = 'pt'").

update_assign_test() ->
	test_parser("UPDATE Test SET name ASSIGN 'aaa' WHERE name = 'a'"),
	test_parser("UPDATE Test SET age ASSIGN 50 WHERE name = 'aa'").

update_increment_test() ->
	test_parser("UPDATE Test SET countCars INCREMENT WHERE model = 'b'"),
	test_parser("UPDATE Test SET countCars INCREMENT 2 WHERE model = 'b'").

update_decrement_test() ->
	test_parser("UPDATE Test SET countCars DECREMENT WHERE model = 'b'"),
	test_parser("UPDATE Test SET countCars DECREMENT 2 WHERE model = 'b'").

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
    test_parser("ABORT TRANSACTION").

quit_test() ->
    test_parser("QUIT"),
    test_parser("quit").

-endif.

-file("/usr/local/lib/erlang/lib/parsetools-2.1.4/include/yeccpre.hrl", 0).
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2015. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% The parser generator will insert appropriate declarations before this line.%

-type yecc_ret() :: {'error', _} | {'ok', _}.

-spec parse(Tokens :: list()) -> yecc_ret().
parse(Tokens) ->
    yeccpars0(Tokens, {no_func, no_line}, 0, [], []).

-spec parse_and_scan({function() | {atom(), atom()}, [_]}
                     | {atom(), atom(), [_]}) -> yecc_ret().
parse_and_scan({F, A}) ->
    yeccpars0([], {{F, A}, no_line}, 0, [], []);
parse_and_scan({M, F, A}) ->
    Arity = length(A),
    yeccpars0([], {{fun M:F/Arity, A}, no_line}, 0, [], []).

-spec format_error(any()) -> [char() | list()].
format_error(Message) ->
    case io_lib:deep_char_list(Message) of
        true ->
            Message;
        _ ->
            io_lib:write(Message)
    end.

%% To be used in grammar files to throw an error message to the parser
%% toplevel. Doesn't have to be exported!
-compile({nowarn_unused_function, return_error/2}).
-spec return_error(integer(), any()) -> no_return().
return_error(Line, Message) ->
    throw({error, {Line, ?MODULE, Message}}).

-define(CODE_VERSION, "1.4").

yeccpars0(Tokens, Tzr, State, States, Vstack) ->
    try yeccpars1(Tokens, Tzr, State, States, Vstack)
    catch 
        error: Error ->
            Stacktrace = erlang:get_stacktrace(),
            try yecc_error_type(Error, Stacktrace) of
                Desc ->
                    erlang:raise(error, {yecc_bug, ?CODE_VERSION, Desc},
                                 Stacktrace)
            catch _:_ -> erlang:raise(error, Error, Stacktrace)
            end;
        %% Probably thrown from return_error/2:
        throw: {error, {_Line, ?MODULE, _M}} = Error ->
            Error
    end.

yecc_error_type(function_clause, [{?MODULE,F,ArityOrArgs,_} | _]) ->
    case atom_to_list(F) of
        "yeccgoto_" ++ SymbolL ->
            {ok,[{atom,_,Symbol}],_} = erl_scan:string(SymbolL),
            State = case ArityOrArgs of
                        [S,_,_,_,_,_,_] -> S;
                        _ -> state_is_unknown
                    end,
            {Symbol, State, missing_in_goto_table}
    end.

yeccpars1([Token | Tokens], Tzr, State, States, Vstack) ->
    yeccpars2(State, element(1, Token), States, Vstack, Token, Tokens, Tzr);
yeccpars1([], {{F, A},_Line}, State, States, Vstack) ->
    case apply(F, A) of
        {ok, Tokens, Endline} ->
            yeccpars1(Tokens, {{F, A}, Endline}, State, States, Vstack);
        {eof, Endline} ->
            yeccpars1([], {no_func, Endline}, State, States, Vstack);
        {error, Descriptor, _Endline} ->
            {error, Descriptor}
    end;
yeccpars1([], {no_func, no_line}, State, States, Vstack) ->
    Line = 999999,
    yeccpars2(State, '$end', States, Vstack, yecc_end(Line), [],
              {no_func, Line});
yeccpars1([], {no_func, Endline}, State, States, Vstack) ->
    yeccpars2(State, '$end', States, Vstack, yecc_end(Endline), [],
              {no_func, Endline}).

%% yeccpars1/7 is called from generated code.
%%
%% When using the {includefile, Includefile} option, make sure that
%% yeccpars1/7 can be found by parsing the file without following
%% include directives. yecc will otherwise assume that an old
%% yeccpre.hrl is included (one which defines yeccpars1/5).
yeccpars1(State1, State, States, Vstack, Token0, [Token | Tokens], Tzr) ->
    yeccpars2(State, element(1, Token), [State1 | States],
              [Token0 | Vstack], Token, Tokens, Tzr);
yeccpars1(State1, State, States, Vstack, Token0, [], {{_F,_A}, _Line}=Tzr) ->
    yeccpars1([], Tzr, State, [State1 | States], [Token0 | Vstack]);
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, no_line}) ->
    Line = yecctoken_end_location(Token0),
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line});
yeccpars1(State1, State, States, Vstack, Token0, [], {no_func, Line}) ->
    yeccpars2(State, '$end', [State1 | States], [Token0 | Vstack],
              yecc_end(Line), [], {no_func, Line}).

%% For internal use only.
yecc_end({Line,_Column}) ->
    {'$end', Line};
yecc_end(Line) ->
    {'$end', Line}.

yecctoken_end_location(Token) ->
    try erl_anno:end_location(element(2, Token)) of
        undefined -> yecctoken_location(Token);
        Loc -> Loc
    catch _:_ -> yecctoken_location(Token)
    end.

-compile({nowarn_unused_function, yeccerror/1}).
yeccerror(Token) ->
    Text = yecctoken_to_string(Token),
    Location = yecctoken_location(Token),
    {error, {Location, ?MODULE, ["syntax error before: ", Text]}}.

-compile({nowarn_unused_function, yecctoken_to_string/1}).
yecctoken_to_string(Token) ->
    try erl_scan:text(Token) of
        undefined -> yecctoken2string(Token);
        Txt -> Txt
    catch _:_ -> yecctoken2string(Token)
    end.

yecctoken_location(Token) ->
    try erl_scan:location(Token)
    catch _:_ -> element(2, Token)
    end.

-compile({nowarn_unused_function, yecctoken2string/1}).
yecctoken2string({atom, _, A}) -> io_lib:write(A);
yecctoken2string({integer,_,N}) -> io_lib:write(N);
yecctoken2string({float,_,F}) -> io_lib:write(F);
yecctoken2string({char,_,C}) -> io_lib:write_char(C);
yecctoken2string({var,_,V}) -> io_lib:format("~s", [V]);
yecctoken2string({string,_,S}) -> io_lib:write_string(S);
yecctoken2string({reserved_symbol, _, A}) -> io_lib:write(A);
yecctoken2string({_Cat, _, Val}) -> io_lib:format("~p",[Val]);
yecctoken2string({dot, _}) -> "'.'";
yecctoken2string({'$end', _}) ->
    [];
yecctoken2string({Other, _}) when is_atom(Other) ->
    io_lib:write(Other);
yecctoken2string(Other) ->
    io_lib:write(Other).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



-file("src/parser.erl", 297).

-dialyzer({nowarn_function, yeccpars2/7}).
yeccpars2(0=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_0(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(1=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_1(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(2=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_2(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(3=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_3(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(4=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_4(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(5=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_5(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(6=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_6(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(7=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_7(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(8=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_8(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(9=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_9(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(10=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_10(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(11=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_11(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(12=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_12(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(13=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_13(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(14=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_14(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(15=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_15(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(16=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_16(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(17=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_17(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(18=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_18(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(19=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_19(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(20=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_20(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(21=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_21(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(22=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_22(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(23=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(24=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_24(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(25=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_25(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(26=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_26(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(27=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(28=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_28(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(29=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_29(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(30=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_30(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(31=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(32=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_32(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(33=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_33(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(34=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_34(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(35=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_35(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(36=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_36(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(37=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_37(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(38=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_38(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(39=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_39(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(40=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_40(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(41=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(42=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_42(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(43=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(44=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_44(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(45=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_45(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(46=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_46(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(47=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(48=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_48(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(49=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(50=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(51=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_51(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(52=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(53=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(54=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_54(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(55=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_55(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(56=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_56(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(57=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_57(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(58=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_58(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(59=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(60=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_60(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(61=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_61(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(62=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_62(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(63=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_63(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(64=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_64(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(65=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_65(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(66=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_66(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(67=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_67(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(68=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_68(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(69=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_69(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(70=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(71=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_71(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(72=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_72(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(73=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(74=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_74(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(75=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(76=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_76(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(77=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_77(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(78=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_78(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(79=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_79(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(80=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_80(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(81=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(82=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_82(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(83=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(84=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_84(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(85=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(86=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_86(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(87=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(88=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_88(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(89=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_89(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(90=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(91=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_91(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(92=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_92(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(93=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(94=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_94(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(95=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_95(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(96=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_96(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(97=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(98=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_98(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(99=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_99(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(100=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_100(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(101=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_101(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(102=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(103=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_103(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(104=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_91(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(105=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_105(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(106=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(107=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_107(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(108=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_43(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(109=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_109(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(110=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(111=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_111(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(112=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(113=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_113(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(114=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(115=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_115(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(116=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_116(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(117=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_117(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(118=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_118(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(119=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_119(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(120=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_120(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(121=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_121(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(122=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(123=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_123(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(124=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_124(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(125=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_125(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(126=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_126(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(127=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_127(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(128=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(129=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_129(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(130=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(131=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_131(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(132=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_132(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(133=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_133(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(134=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_134(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(135=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_135(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(136=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_136(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(137=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(138=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_138(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(139=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_139(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(140=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_140(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(141=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_141(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(142=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_142(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(143=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_143(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(144=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_144(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(145=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_145(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(146=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_146(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(147=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(148=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_148(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(149=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_149(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(150=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_150(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(151=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(152=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_152(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(153=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_153(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(154=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_154(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(155=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(156=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_156(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(157=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(158=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_158(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(159=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_159(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(160=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_160(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(161=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_23(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(162=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_162(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(163=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_163(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(164=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_164(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(165=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_165(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(166=S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_166(S, Cat, Ss, Stack, T, Ts, Tzr);
%% yeccpars2(167=S, Cat, Ss, Stack, T, Ts, Tzr) ->
%%  yeccpars2_167(S, Cat, Ss, Stack, T, Ts, Tzr);
yeccpars2(Other, _, _, _, _, _, _) ->
 erlang:error({yecc_bug,"1.4",{missing_state_in_action_table, Other}}).

-dialyzer({nowarn_function, yeccpars2_0/7}).
yeccpars2_0(S, abort, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 14, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, 'begin', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, commit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, create, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 17, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, delete, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 18, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, insert, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, quit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 20, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 21, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, show, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 22, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(S, update, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 23, Ss, Stack, T, Ts, Tzr);
yeccpars2_0(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_1_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_2(S, semi_colon, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 166, Ss, Stack, T, Ts, Tzr);
yeccpars2_2(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_query(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_3(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_3_(Stack),
 yeccgoto_admin(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_4(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_4_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_5(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_5_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_6/7}).
yeccpars2_6(_S, '$end', _Ss, Stack, _T, _Ts, _Tzr) ->
 {ok, hd(Stack)};
yeccpars2_6(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_7_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_8_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_9_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_10_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_11_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_statement(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_13(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_13_(Stack),
 yeccgoto_statement(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_14/7}).
yeccpars2_14(S, transaction, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 165, Ss, Stack, T, Ts, Tzr);
yeccpars2_14(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_15/7}).
yeccpars2_15(S, transaction, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 164, Ss, Stack, T, Ts, Tzr);
yeccpars2_15(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_16/7}).
yeccpars2_16(S, transaction, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 163, Ss, Stack, T, Ts, Tzr);
yeccpars2_16(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_17/7}).
yeccpars2_17(S, index, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 110, Ss, Stack, T, Ts, Tzr);
yeccpars2_17(S, table_policy, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 111, Ss, Stack, T, Ts, Tzr);
yeccpars2_17(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_18/7}).
yeccpars2_18(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 106, Ss, Stack, T, Ts, Tzr);
yeccpars2_18(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_19/7}).
yeccpars2_19(S, into, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 87, Ss, Stack, T, Ts, Tzr);
yeccpars2_19(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_20(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_20_(Stack),
 yeccgoto_quit_program(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_21/7}).
yeccpars2_21(S, atom_value, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 25, Ss, Stack, T, Ts, Tzr);
yeccpars2_21(S, wildcard, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 80, Ss, Stack, T, Ts, Tzr);
yeccpars2_21(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_22/7}).
yeccpars2_22(S, index, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 67, Ss, Stack, T, Ts, Tzr);
yeccpars2_22(S, indexes, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 68, Ss, Stack, T, Ts, Tzr);
yeccpars2_22(S, tables, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 69, Ss, Stack, T, Ts, Tzr);
yeccpars2_22(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_23/7}).
yeccpars2_23(S, atom_value, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 25, Ss, Stack, T, Ts, Tzr);
yeccpars2_23(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_24/7}).
yeccpars2_24(S, set, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 27, Ss, Stack, T, Ts, Tzr);
yeccpars2_24(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_25(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_25_(Stack),
 yeccgoto_atom(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_26(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 43, Ss, Stack, T, Ts, Tzr);
yeccpars2_26(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_26_(Stack),
 yeccgoto_update_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_27: see yeccpars2_23

yeccpars2_28(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 41, Ss, Stack, T, Ts, Tzr);
yeccpars2_28(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_28_(Stack),
 yeccgoto_set_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_29(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_29_(Stack),
 yeccgoto_set_assignments(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_30/7}).
yeccpars2_30(S, assign, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 31, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(S, decrement, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 32, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(S, increment, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 33, Ss, Stack, T, Ts, Tzr);
yeccpars2_30(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_31/7}).
yeccpars2_31(S, boolean, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 38, Ss, Stack, T, Ts, Tzr);
yeccpars2_31(S, number, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 39, Ss, Stack, T, Ts, Tzr);
yeccpars2_31(S, string, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 40, Ss, Stack, T, Ts, Tzr);
yeccpars2_31(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_32(S, number, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 35, Ss, Stack, T, Ts, Tzr);
yeccpars2_32(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_32_(Stack),
 yeccgoto_set_assignment(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_33(S, number, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 35, Ss, Stack, T, Ts, Tzr);
yeccpars2_33(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_33_(Stack),
 yeccgoto_set_assignment(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_34(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_34_(Stack),
 yeccgoto_set_assignment(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_35(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_35_(Stack),
 yeccgoto_number_unwrap(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_36(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_36_(Stack),
 yeccgoto_set_assignment(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_37(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_37_(Stack),
 yeccgoto_set_assignment(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_38(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_38_(Stack),
 yeccgoto_value(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_39(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_39_(Stack),
 yeccgoto_value(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_40(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_40_(Stack),
 yeccgoto_value(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

%% yeccpars2_41: see yeccpars2_23

yeccpars2_42(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_42_(Stack),
 yeccgoto_set_assignments(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_43/7}).
yeccpars2_43(S, atom_value, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 25, Ss, Stack, T, Ts, Tzr);
yeccpars2_43(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 47, Ss, Stack, T, Ts, Tzr);
yeccpars2_43(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_44(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_44(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_44(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_44_(Stack),
 yeccgoto_update_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_45_(Stack),
 yeccgoto_where_clauses(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_46/7}).
yeccpars2_46(S, equality, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 60, Ss, Stack, T, Ts, Tzr);
yeccpars2_46(S, greater, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 61, Ss, Stack, T, Ts, Tzr);
yeccpars2_46(S, greatereq, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 62, Ss, Stack, T, Ts, Tzr);
yeccpars2_46(S, lesser, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 63, Ss, Stack, T, Ts, Tzr);
yeccpars2_46(S, lessereq, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 64, Ss, Stack, T, Ts, Tzr);
yeccpars2_46(S, notequality, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 65, Ss, Stack, T, Ts, Tzr);
yeccpars2_46(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_47: see yeccpars2_43

-dialyzer({nowarn_function, yeccpars2_48/7}).
yeccpars2_48(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 49, Ss, Stack, T, Ts, Tzr);
yeccpars2_48(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 50, Ss, Stack, T, Ts, Tzr);
yeccpars2_48(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_49: see yeccpars2_43

%% yeccpars2_50: see yeccpars2_43

-dialyzer({nowarn_function, yeccpars2_51/7}).
yeccpars2_51(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_51(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_51(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 54, Ss, Stack, T, Ts, Tzr);
yeccpars2_51(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_52: see yeccpars2_43

%% yeccpars2_53: see yeccpars2_43

yeccpars2_54(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_54_(Stack),
 yeccgoto_where_clauses(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_55(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_55(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_55_(Stack),
 yeccgoto_where_clauses(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_56(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_56(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_56(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_56_(Stack),
 yeccgoto_where_clauses(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_57/7}).
yeccpars2_57(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_57(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_57(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 58, Ss, Stack, T, Ts, Tzr);
yeccpars2_57(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_58(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_58_(Stack),
 yeccgoto_where_clauses(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_59: see yeccpars2_31

yeccpars2_60(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_comparison(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_61(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_comparison(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_62(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_comparison(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_63(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_comparison(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_64(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_comparison(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_65(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_comparison(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_66(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_66_(Stack),
 yeccgoto_where_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_67/7}).
yeccpars2_67(S, atom_value, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 25, Ss, Stack, T, Ts, Tzr);
yeccpars2_67(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 73, Ss, Stack, T, Ts, Tzr);
yeccpars2_67(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_68/7}).
yeccpars2_68(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 70, Ss, Stack, T, Ts, Tzr);
yeccpars2_68(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_69(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_69_(Stack),
 yeccgoto_show_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_70: see yeccpars2_23

yeccpars2_71(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_71_(Stack),
 yeccgoto_show_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_72/7}).
yeccpars2_72(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 75, Ss, Stack, T, Ts, Tzr);
yeccpars2_72(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_73: see yeccpars2_23

yeccpars2_74(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_74_(Stack),
 yeccgoto_show_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_75: see yeccpars2_23

yeccpars2_76(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_76_(Stack),
 yeccgoto_show_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_77(S, sep, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 85, Ss, Stack, T, Ts, Tzr);
yeccpars2_77(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_projection(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_78/7}).
yeccpars2_78(S, from, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 81, Ss, Stack, T, Ts, Tzr);
yeccpars2_78(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_79(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_79_(Stack),
 yeccgoto_select_fields(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_80(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_projection(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

%% yeccpars2_81: see yeccpars2_23

yeccpars2_82(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 83, Ss, Stack, T, Ts, Tzr);
yeccpars2_82(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_|Nss] = Ss,
 NewStack = yeccpars2_82_(Stack),
 yeccgoto_select_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_83: see yeccpars2_43

yeccpars2_84(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_84(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_84(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_84_(Stack),
 yeccgoto_select_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_85: see yeccpars2_23

yeccpars2_86(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_86_(Stack),
 yeccgoto_select_fields(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_87: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_88/7}).
yeccpars2_88(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 90, Ss, Stack, T, Ts, Tzr);
yeccpars2_88(S, values, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 91, Ss, Stack, T, Ts, Tzr);
yeccpars2_88(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_89/7}).
yeccpars2_89(S, values, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 104, Ss, Stack, T, Ts, Tzr);
yeccpars2_89(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_90: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_91/7}).
yeccpars2_91(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 93, Ss, Stack, T, Ts, Tzr);
yeccpars2_91(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_92(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_92_(Stack),
 yeccgoto_insert_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_93: see yeccpars2_31

yeccpars2_94(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_94_(Stack),
 yeccgoto_insert_values(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_95/7}).
yeccpars2_95(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 96, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(S, sep, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 97, Ss, Stack, T, Ts, Tzr);
yeccpars2_95(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_96(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_96_(Stack),
 yeccgoto_insert_values_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_97: see yeccpars2_31

yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_98_(Stack),
 yeccgoto_insert_values(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_99/7}).
yeccpars2_99(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 101, Ss, Stack, T, Ts, Tzr);
yeccpars2_99(S, sep, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 102, Ss, Stack, T, Ts, Tzr);
yeccpars2_99(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_100(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_100_(Stack),
 yeccgoto_insert_keys(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_101(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_101_(Stack),
 yeccgoto_insert_keys_clause(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_102: see yeccpars2_23

yeccpars2_103(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_103_(Stack),
 yeccgoto_insert_keys(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_104: see yeccpars2_91

yeccpars2_105(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_105_(Stack),
 yeccgoto_insert_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_106: see yeccpars2_23

yeccpars2_107(S, where, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 108, Ss, Stack, T, Ts, Tzr);
yeccpars2_107(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_107_(Stack),
 yeccgoto_delete_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_108: see yeccpars2_43

yeccpars2_109(S, conjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 52, Ss, Stack, T, Ts, Tzr);
yeccpars2_109(S, disjunctive, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 53, Ss, Stack, T, Ts, Tzr);
yeccpars2_109(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_109_(Stack),
 yeccgoto_delete_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_110: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_111/7}).
yeccpars2_111(S, table, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 112, Ss, Stack, T, Ts, Tzr);
yeccpars2_111(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_112: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_113/7}).
yeccpars2_113(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 114, Ss, Stack, T, Ts, Tzr);
yeccpars2_113(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_114: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_115/7}).
yeccpars2_115(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 146, Ss, Stack, T, Ts, Tzr);
yeccpars2_115(S, sep, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 147, Ss, Stack, T, Ts, Tzr);
yeccpars2_115(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_116/7}).
yeccpars2_116(S, attribute_type, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 119, Ss, Stack, T, Ts, Tzr);
yeccpars2_116(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_117(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_117_(Stack),
 yeccgoto_create_keys(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_118(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccgoto_attribute_name(hd(Ss), Cat, Ss, Stack, T, Ts, Tzr).

yeccpars2_119(S, check, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 121, Ss, Stack, T, Ts, Tzr);
yeccpars2_119(S, default, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 122, Ss, Stack, T, Ts, Tzr);
yeccpars2_119(S, foreign, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 123, Ss, Stack, T, Ts, Tzr);
yeccpars2_119(S, primary, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 124, Ss, Stack, T, Ts, Tzr);
yeccpars2_119(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_119_(Stack),
 yeccgoto_attribute(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_120(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_120_(Stack),
 yeccgoto_attribute(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_121/7}).
yeccpars2_121(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 137, Ss, Stack, T, Ts, Tzr);
yeccpars2_121(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_122: see yeccpars2_31

-dialyzer({nowarn_function, yeccpars2_123/7}).
yeccpars2_123(S, key, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 126, Ss, Stack, T, Ts, Tzr);
yeccpars2_123(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_124/7}).
yeccpars2_124(S, key, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 125, Ss, Stack, T, Ts, Tzr);
yeccpars2_124(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_125(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_125_(Stack),
 yeccgoto_attribute_constraint(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_126/7}).
yeccpars2_126(S, dep_policy, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 127, Ss, Stack, T, Ts, Tzr);
yeccpars2_126(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_127/7}).
yeccpars2_127(S, references, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 128, Ss, Stack, T, Ts, Tzr);
yeccpars2_127(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_128: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_129/7}).
yeccpars2_129(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 130, Ss, Stack, T, Ts, Tzr);
yeccpars2_129(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_130: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_131/7}).
yeccpars2_131(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 132, Ss, Stack, T, Ts, Tzr);
yeccpars2_131(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_132(S, on, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 133, Ss, Stack, T, Ts, Tzr);
yeccpars2_132(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_132_(Stack),
 yeccgoto_attribute_constraint(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_133/7}).
yeccpars2_133(S, delete, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 134, Ss, Stack, T, Ts, Tzr);
yeccpars2_133(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_134/7}).
yeccpars2_134(S, cascade, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 135, Ss, Stack, T, Ts, Tzr);
yeccpars2_134(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_135(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_135_(Stack),
 yeccgoto_attribute_constraint(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_136(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_136_(Stack),
 yeccgoto_attribute_constraint(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_137: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_138/7}).
yeccpars2_138(S, greater, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 140, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(S, greatereq, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 141, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(S, lesser, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 142, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(S, lessereq, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 143, Ss, Stack, T, Ts, Tzr);
yeccpars2_138(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_139/7}).
yeccpars2_139(S, number, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 35, Ss, Stack, T, Ts, Tzr);
yeccpars2_139(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_140(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_140_(Stack),
 yeccgoto_check_comparator(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_141(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_141_(Stack),
 yeccgoto_check_comparator(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_142(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_142_(Stack),
 yeccgoto_check_comparator(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_143(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_143_(Stack),
 yeccgoto_check_comparator(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_144/7}).
yeccpars2_144(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 145, Ss, Stack, T, Ts, Tzr);
yeccpars2_144(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_145(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_145_(Stack),
 yeccgoto_attribute_constraint(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_146(S, partition, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 149, Ss, Stack, T, Ts, Tzr);
yeccpars2_146(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_146_(Stack),
 yeccgoto_create_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_147: see yeccpars2_23

yeccpars2_148(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_148_(Stack),
 yeccgoto_create_keys(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_149/7}).
yeccpars2_149(S, on, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 150, Ss, Stack, T, Ts, Tzr);
yeccpars2_149(_, _, _, _, T, _, _) ->
 yeccerror(T).

-dialyzer({nowarn_function, yeccpars2_150/7}).
yeccpars2_150(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 151, Ss, Stack, T, Ts, Tzr);
yeccpars2_150(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_151: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_152/7}).
yeccpars2_152(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 153, Ss, Stack, T, Ts, Tzr);
yeccpars2_152(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_153(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_153_(Stack),
 yeccgoto_create_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccpars2_154/7}).
yeccpars2_154(S, on, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 155, Ss, Stack, T, Ts, Tzr);
yeccpars2_154(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_155: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_156/7}).
yeccpars2_156(S, start_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 157, Ss, Stack, T, Ts, Tzr);
yeccpars2_156(_, _, _, _, T, _, _) ->
 yeccerror(T).

%% yeccpars2_157: see yeccpars2_23

-dialyzer({nowarn_function, yeccpars2_158/7}).
yeccpars2_158(S, end_list, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 160, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(S, sep, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 161, Ss, Stack, T, Ts, Tzr);
yeccpars2_158(_, _, _, _, T, _, _) ->
 yeccerror(T).

yeccpars2_159(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 NewStack = yeccpars2_159_(Stack),
 yeccgoto_create_index_keys(hd(Ss), Cat, Ss, NewStack, T, Ts, Tzr).

yeccpars2_160(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_,_,_,_,_,_|Nss] = Ss,
 NewStack = yeccpars2_160_(Stack),
 yeccgoto_create_query(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

%% yeccpars2_161: see yeccpars2_23

yeccpars2_162(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_162_(Stack),
 yeccgoto_create_index_keys(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_163(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_163_(Stack),
 yeccgoto_commit_transaction(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_164(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_164_(Stack),
 yeccgoto_begin_transaction(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_165(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_165_(Stack),
 yeccgoto_abort_transaction(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_166(S, abort, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 14, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, 'begin', Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 15, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, commit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 16, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, create, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 17, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, delete, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 18, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, insert, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 19, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, quit, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 20, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, select, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 21, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, show, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 22, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(S, update, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 23, Ss, Stack, T, Ts, Tzr);
yeccpars2_166(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_|Nss] = Ss,
 NewStack = yeccpars2_166_(Stack),
 yeccgoto_statement(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

yeccpars2_167(S, semi_colon, Ss, Stack, T, Ts, Tzr) ->
 yeccpars1(S, 166, Ss, Stack, T, Ts, Tzr);
yeccpars2_167(_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 [_,_|Nss] = Ss,
 NewStack = yeccpars2_167_(Stack),
 yeccgoto_statement(hd(Nss), Cat, Nss, NewStack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_abort_transaction/7}).
yeccgoto_abort_transaction(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_13(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_abort_transaction(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_13(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_admin/7}).
yeccgoto_admin(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_admin(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_12(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_atom/7}).
yeccgoto_atom(21=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_79(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(23, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_24(24, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(27, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_30(30, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(41, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_30(30, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(43, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(47, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(49, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(50, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(52, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(53, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(67, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_72(72, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(70=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_71(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(73=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_74(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(75=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_76(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(81, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_82(82, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(83, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(85=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_86(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(87, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_88(88, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(90=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_100(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(102=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_103(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(106, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_107(107, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(108, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_46(46, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(110, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_154(154, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(112, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_113(113, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_118(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(128, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_129(129, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(130, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_131(131, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(137=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_118(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(147=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_118(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(151=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_118(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(155, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_156(156, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(157=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_159(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_atom(161=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_162(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_attribute/7}).
yeccgoto_attribute(114=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_117(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_attribute(147=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_148(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_attribute_constraint/7}).
yeccgoto_attribute_constraint(119=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_120(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_attribute_name/7}).
yeccgoto_attribute_name(114, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_116(116, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_attribute_name(137, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_138(138, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_attribute_name(147, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_116(116, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_attribute_name(151, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_152(152, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_begin_transaction/7}).
yeccgoto_begin_transaction(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_begin_transaction(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_11(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_check_comparator/7}).
yeccgoto_check_comparator(138, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_139(139, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_commit_transaction/7}).
yeccgoto_commit_transaction(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_commit_transaction(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_10(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_comparison/7}).
yeccgoto_comparison(46, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_31(59, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_create_index_keys/7}).
yeccgoto_create_index_keys(157, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_158(158, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_create_keys/7}).
yeccgoto_create_keys(114, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_115(115, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_create_query/7}).
yeccgoto_create_query(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_create_query(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_9(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_delete_query/7}).
yeccgoto_delete_query(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_delete_query(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_8(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_keys/7}).
yeccgoto_insert_keys(90, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_99(99, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_keys_clause/7}).
yeccgoto_insert_keys_clause(88, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_89(89, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_query/7}).
yeccgoto_insert_query(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_insert_query(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_7(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_values/7}).
yeccgoto_insert_values(93, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_95(95, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_insert_values_clause/7}).
yeccgoto_insert_values_clause(91=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_92(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_insert_values_clause(104=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_105(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_number_unwrap/7}).
yeccgoto_number_unwrap(32=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_36(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_number_unwrap(33=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_34(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_number_unwrap(139, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_144(144, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_projection/7}).
yeccgoto_projection(21, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_78(78, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_query/7}).
yeccgoto_query(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_6(6, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_quit_program/7}).
yeccgoto_quit_program(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_5(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_quit_program(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_5(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_select_fields/7}).
yeccgoto_select_fields(21, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_77(77, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_select_query/7}).
yeccgoto_select_query(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_4(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_select_query(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_4(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_assignment/7}).
yeccgoto_set_assignment(27=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_29(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_set_assignment(41=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_42(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_assignments/7}).
yeccgoto_set_assignments(27, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_28(28, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_set_clause/7}).
yeccgoto_set_clause(24, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_26(26, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_show_query/7}).
yeccgoto_show_query(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_show_query(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_3(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_statement/7}).
yeccgoto_statement(0, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_2(2, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_statement(166, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_167(167, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_update_query/7}).
yeccgoto_update_query(0=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_update_query(166=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_1(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_value/7}).
yeccgoto_value(31=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_37(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_value(59=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_66(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_value(93=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_94(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_value(97=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_98(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_value(122=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_136(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_where_clause/7}).
yeccgoto_where_clause(43=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(47=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(49=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(50=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(52=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(53=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(83=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clause(108=_S, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_45(_S, Cat, Ss, Stack, T, Ts, Tzr).

-dialyzer({nowarn_function, yeccgoto_where_clauses/7}).
yeccgoto_where_clauses(43, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_44(44, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(47, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_48(48, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(49, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_57(57, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(50, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_51(51, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(52, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_56(56, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(53, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_55(55, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(83, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_84(84, Cat, Ss, Stack, T, Ts, Tzr);
yeccgoto_where_clauses(108, Cat, Ss, Stack, T, Ts, Tzr) ->
 yeccpars2_109(109, Cat, Ss, Stack, T, Ts, Tzr).

-compile({inline,yeccpars2_1_/1}).
-file("src/parser.yrl", 85).
yeccpars2_1_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_3_/1}).
-file("src/parser.yrl", 104).
yeccpars2_3_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_4_/1}).
-file("src/parser.yrl", 79).
yeccpars2_4_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_5_/1}).
-file("src/parser.yrl", 95).
yeccpars2_5_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_7_/1}).
-file("src/parser.yrl", 81).
yeccpars2_7_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_8_/1}).
-file("src/parser.yrl", 83).
yeccpars2_8_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_9_/1}).
-file("src/parser.yrl", 87).
yeccpars2_9_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_10_/1}).
-file("src/parser.yrl", 91).
yeccpars2_10_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_11_/1}).
-file("src/parser.yrl", 89).
yeccpars2_11_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_13_/1}).
-file("src/parser.yrl", 93).
yeccpars2_13_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-file("src/parser.erl", 1878).
-compile({inline,yeccpars2_20_/1}).
-file("src/parser.yrl", 369).
yeccpars2_20_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   ? QUIT_CLAUSE ( ? QUIT_TOKEN )
  end | __Stack].

-compile({inline,yeccpars2_25_/1}).
-file("src/parser.yrl", 376).
yeccpars2_25_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   unwrap_type ( __1 )
  end | __Stack].

-file("src/parser.erl", 1895).
-compile({inline,yeccpars2_26_/1}).
-file("src/parser.yrl", 228).
yeccpars2_26_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? UPDATE_CLAUSE ( { __2 , __3 , ? PARSER_WILDCARD } )
  end | __Stack].

-file("src/parser.erl", 1904).
-compile({inline,yeccpars2_28_/1}).
-file("src/parser.yrl", 236).
yeccpars2_28_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? SET_CLAUSE ( __2 )
  end | __Stack].

-compile({inline,yeccpars2_29_/1}).
-file("src/parser.yrl", 244).
yeccpars2_29_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_32_/1}).
-file("src/parser.yrl", 262).
yeccpars2_32_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 , 1 }
  end | __Stack].

-compile({inline,yeccpars2_33_/1}).
-file("src/parser.yrl", 254).
yeccpars2_33_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 , 1 }
  end | __Stack].

-compile({inline,yeccpars2_34_/1}).
-file("src/parser.yrl", 258).
yeccpars2_34_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 , __3 }
  end | __Stack].

-compile({inline,yeccpars2_35_/1}).
-file("src/parser.yrl", 380).
yeccpars2_35_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   unwrap_type ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_36_/1}).
-file("src/parser.yrl", 266).
yeccpars2_36_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 , __3 }
  end | __Stack].

-compile({inline,yeccpars2_37_/1}).
-file("src/parser.yrl", 249).
yeccpars2_37_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 , __3 }
  end | __Stack].

-compile({inline,yeccpars2_38_/1}).
-file("src/parser.yrl", 392).
yeccpars2_38_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   unwrap_type ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_39_/1}).
-file("src/parser.yrl", 384).
yeccpars2_39_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   unwrap_type ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_40_/1}).
-file("src/parser.yrl", 388).
yeccpars2_40_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   unwrap_type ( __1 )
  end | __Stack].

-compile({inline,yeccpars2_42_/1}).
-file("src/parser.yrl", 240).
yeccpars2_42_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , [ __3 ] )
  end | __Stack].

-file("src/parser.erl", 2001).
-compile({inline,yeccpars2_44_/1}).
-file("src/parser.yrl", 232).
yeccpars2_44_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? UPDATE_CLAUSE ( { __2 , __3 , __5 } )
  end | __Stack].

-compile({inline,yeccpars2_45_/1}).
-file("src/parser.yrl", 174).
yeccpars2_45_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_54_/1}).
-file("src/parser.yrl", 170).
yeccpars2_54_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ lists : append ( [ __2 , [ __3 ] , __4 ] ) ]
  end | __Stack].

-compile({inline,yeccpars2_55_/1}).
-file("src/parser.yrl", 162).
yeccpars2_55_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( [ __1 , [ __2 ] , __3 ] )
  end | __Stack].

-compile({inline,yeccpars2_56_/1}).
-file("src/parser.yrl", 158).
yeccpars2_56_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( [ __1 , [ __2 ] , __3 ] )
  end | __Stack].

-compile({inline,yeccpars2_58_/1}).
-file("src/parser.yrl", 166).
yeccpars2_58_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   [ lists : append ( [ __2 , [ __3 ] , __4 ] ) ]
  end | __Stack].

-compile({inline,yeccpars2_66_/1}).
-file("src/parser.yrl", 183).
yeccpars2_66_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   { __1 , __2 , __3 }
  end | __Stack].

-file("src/parser.erl", 2058).
-compile({inline,yeccpars2_69_/1}).
-file("src/parser.yrl", 123).
yeccpars2_69_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? SHOW_CLAUSE ( ? TABLES_TOKEN )
  end | __Stack].

-file("src/parser.erl", 2067).
-compile({inline,yeccpars2_71_/1}).
-file("src/parser.yrl", 119).
yeccpars2_71_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? SHOW_CLAUSE ( { ? INDEXES_TOKEN , __4 } )
  end | __Stack].

-file("src/parser.erl", 2076).
-compile({inline,yeccpars2_74_/1}).
-file("src/parser.yrl", 111).
yeccpars2_74_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? SHOW_CLAUSE ( { ? INDEX_TOKEN , __4 } )
  end | __Stack].

-file("src/parser.erl", 2085).
-compile({inline,yeccpars2_76_/1}).
-file("src/parser.yrl", 115).
yeccpars2_76_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? SHOW_CLAUSE ( { ? INDEX_TOKEN , __3 , __5 } )
  end | __Stack].

-compile({inline,yeccpars2_79_/1}).
-file("src/parser.yrl", 150).
yeccpars2_79_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-file("src/parser.erl", 2102).
-compile({inline,yeccpars2_82_/1}).
-file("src/parser.yrl", 130).
yeccpars2_82_(__Stack0) ->
 [__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? SELECT_CLAUSE ( { __4 , __2 , ? PARSER_WILDCARD } )
  end | __Stack].

-file("src/parser.erl", 2111).
-compile({inline,yeccpars2_84_/1}).
-file("src/parser.yrl", 134).
yeccpars2_84_(__Stack0) ->
 [__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? SELECT_CLAUSE ( { __4 , __2 , __6 } )
  end | __Stack].

-compile({inline,yeccpars2_86_/1}).
-file("src/parser.yrl", 146).
yeccpars2_86_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , [ __3 ] )
  end | __Stack].

-file("src/parser.erl", 2128).
-compile({inline,yeccpars2_92_/1}).
-file("src/parser.yrl", 196).
yeccpars2_92_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? INSERT_CLAUSE ( { __3 , ? PARSER_WILDCARD , __5 } )
  end | __Stack].

-compile({inline,yeccpars2_94_/1}).
-file("src/parser.yrl", 220).
yeccpars2_94_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_96_/1}).
-file("src/parser.yrl", 212).
yeccpars2_96_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_98_/1}).
-file("src/parser.yrl", 216).
yeccpars2_98_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , [ __3 ] )
  end | __Stack].

-compile({inline,yeccpars2_100_/1}).
-file("src/parser.yrl", 208).
yeccpars2_100_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-compile({inline,yeccpars2_101_/1}).
-file("src/parser.yrl", 200).
yeccpars2_101_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   __2
  end | __Stack].

-compile({inline,yeccpars2_103_/1}).
-file("src/parser.yrl", 204).
yeccpars2_103_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , [ __3 ] )
  end | __Stack].

-file("src/parser.erl", 2185).
-compile({inline,yeccpars2_105_/1}).
-file("src/parser.yrl", 191).
yeccpars2_105_(__Stack0) ->
 [__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? INSERT_CLAUSE ( { __3 , __4 , __6 } )
  end | __Stack].

-file("src/parser.erl", 2194).
-compile({inline,yeccpars2_107_/1}).
-file("src/parser.yrl", 342).
yeccpars2_107_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? DELETE_CLAUSE ( { __3 , ? PARSER_WILDCARD } )
  end | __Stack].

-file("src/parser.erl", 2203).
-compile({inline,yeccpars2_109_/1}).
-file("src/parser.yrl", 346).
yeccpars2_109_(__Stack0) ->
 [__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? DELETE_CLAUSE ( { __3 , __5 } )
  end | __Stack].

-compile({inline,yeccpars2_117_/1}).
-file("src/parser.yrl", 289).
yeccpars2_117_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-file("src/parser.erl", 2220).
-compile({inline,yeccpars2_119_/1}).
-file("src/parser.yrl", 297).
yeccpars2_119_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? T_COL ( __1 , unwrap_type ( __2 ) , ? NO_CONSTRAINT )
  end | __Stack].

-file("src/parser.erl", 2229).
-compile({inline,yeccpars2_120_/1}).
-file("src/parser.yrl", 293).
yeccpars2_120_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? T_COL ( __1 , unwrap_type ( __2 ) , __3 )
  end | __Stack].

-file("src/parser.erl", 2238).
-compile({inline,yeccpars2_125_/1}).
-file("src/parser.yrl", 301).
yeccpars2_125_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? PRIMARY_TOKEN
  end | __Stack].

-file("src/parser.erl", 2247).
-compile({inline,yeccpars2_132_/1}).
-file("src/parser.yrl", 305).
yeccpars2_132_(__Stack0) ->
 [__8,__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? FOREIGN_KEY ( { __5 , __7 , unwrap_type ( __3 ) , ? RESTRICT_TOKEN } )
  end | __Stack].

-file("src/parser.erl", 2256).
-compile({inline,yeccpars2_135_/1}).
-file("src/parser.yrl", 309).
yeccpars2_135_(__Stack0) ->
 [__11,__10,__9,__8,__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? FOREIGN_KEY ( { __5 , __7 , unwrap_type ( __3 ) , ? CASCADE_TOKEN } )
  end | __Stack].

-file("src/parser.erl", 2265).
-compile({inline,yeccpars2_136_/1}).
-file("src/parser.yrl", 313).
yeccpars2_136_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? DEFAULT_KEY ( __2 )
  end | __Stack].

-file("src/parser.erl", 2274).
-compile({inline,yeccpars2_140_/1}).
-file("src/parser.yrl", 323).
yeccpars2_140_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   ? GREATER_KEY
  end | __Stack].

-file("src/parser.erl", 2283).
-compile({inline,yeccpars2_141_/1}).
-file("src/parser.yrl", 325).
yeccpars2_141_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   ? GREATEREQ_KEY
  end | __Stack].

-file("src/parser.erl", 2292).
-compile({inline,yeccpars2_142_/1}).
-file("src/parser.yrl", 324).
yeccpars2_142_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   ? LESSER_KEY
  end | __Stack].

-file("src/parser.erl", 2301).
-compile({inline,yeccpars2_143_/1}).
-file("src/parser.yrl", 326).
yeccpars2_143_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   ? LESSEREQ_KEY
  end | __Stack].

-file("src/parser.erl", 2310).
-compile({inline,yeccpars2_145_/1}).
-file("src/parser.yrl", 317).
yeccpars2_145_(__Stack0) ->
 [__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? CHECK_KEY ( { __3 , __4 , __5 } )
  end | __Stack].

-file("src/parser.erl", 2319).
-compile({inline,yeccpars2_146_/1}).
-file("src/parser.yrl", 273).
yeccpars2_146_(__Stack0) ->
 [__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? CREATE_CLAUSE ( ? T_TABLE ( __4 , crp : set_table_level ( unwrap_type ( __2 ) , crp : new ( ) ) , __6 , [ ] , [ ] , undefined ) )
  end | __Stack].

-compile({inline,yeccpars2_148_/1}).
-file("src/parser.yrl", 285).
yeccpars2_148_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , [ __3 ] )
  end | __Stack].

-file("src/parser.erl", 2336).
-compile({inline,yeccpars2_153_/1}).
-file("src/parser.yrl", 277).
yeccpars2_153_(__Stack0) ->
 [__12,__11,__10,__9,__8,__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? CREATE_CLAUSE ( ? T_TABLE ( __4 , crp : set_table_level ( unwrap_type ( __2 ) , crp : new ( ) ) , __6 , [ ] , [ ] , [ __11 ] ) )
  end | __Stack].

-compile({inline,yeccpars2_159_/1}).
-file("src/parser.yrl", 334).
yeccpars2_159_(__Stack0) ->
 [__1 | __Stack] = __Stack0,
 [begin
   [ __1 ]
  end | __Stack].

-file("src/parser.erl", 2353).
-compile({inline,yeccpars2_160_/1}).
-file("src/parser.yrl", 281).
yeccpars2_160_(__Stack0) ->
 [__8,__7,__6,__5,__4,__3,__2,__1 | __Stack] = __Stack0,
 [begin
   ? CREATE_CLAUSE ( ? T_INDEX ( __3 , __5 , __7 ) )
  end | __Stack].

-compile({inline,yeccpars2_162_/1}).
-file("src/parser.yrl", 330).
yeccpars2_162_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , [ __3 ] )
  end | __Stack].

-file("src/parser.erl", 2370).
-compile({inline,yeccpars2_163_/1}).
-file("src/parser.yrl", 358).
yeccpars2_163_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? COMMIT_CLAUSE ( ? TRANSACTION_TOKEN )
  end | __Stack].

-file("src/parser.erl", 2379).
-compile({inline,yeccpars2_164_/1}).
-file("src/parser.yrl", 354).
yeccpars2_164_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? BEGIN_CLAUSE ( ? TRANSACTION_TOKEN )
  end | __Stack].

-file("src/parser.erl", 2388).
-compile({inline,yeccpars2_165_/1}).
-file("src/parser.yrl", 362).
yeccpars2_165_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   ? ABORT_CLAUSE ( ? TRANSACTION_TOKEN )
  end | __Stack].

-compile({inline,yeccpars2_166_/1}).
-file("src/parser.yrl", 75).
yeccpars2_166_(__Stack0) ->
 [__2,__1 | __Stack] = __Stack0,
 [begin
   __1
  end | __Stack].

-compile({inline,yeccpars2_167_/1}).
-file("src/parser.yrl", 73).
yeccpars2_167_(__Stack0) ->
 [__3,__2,__1 | __Stack] = __Stack0,
 [begin
   lists : append ( __1 , __3 )
  end | __Stack].


-file("src/parser.yrl", 520).

%%%-------------------------------------------------------------------
%%% @author joao
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. ago 2017 14:18
%%%-------------------------------------------------------------------
-module(aql_SUITE).
-author("joao").

-include_lib("aql.hrl").
-include_lib("parser.hrl").
-include_lib("types.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ct_aql.hrl").

-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% API
-export([select_all/1,
    insert_artist/1,
    insert_with_default_artist/1,
    insert_duplicate/1,
    delete_album/1,
    delete_non_existent_album/1,
    commit_transaction/1,
    abort_transaction/1,
    error_transaction/1]).

init_per_suite(Config) ->
<<<<<<< HEAD
    TNameArtist = "ArtistTest",
    TNameAlbum = "AlbumTest",
    TNameTrack = "TrackTest",
    DefaultArtist = 0,
    DefaultAlbum = false,
    {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameArtist,
        " (Name VARCHAR PRIMARY KEY, City VARCHAR,",
        " Awards INTEGER DEFAULT ", DefaultArtist, ");"])),
    {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameAlbum,
        " (Name VARCHAR PRIMARY KEY,",
        " IsSingle BOOLEAN DEFAULT ", DefaultAlbum, ");"])),
    {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameTrack,
        " (Name VARCHAR PRIMARY KEY, Plays COUNTER_INT CHECK (Plays > 0));"
    ])),
    lists:append(Config, [
        {tname_artist, TNameArtist},
        {tname_album, TNameAlbum},
        {tname_track, TNameTrack},
        {default_artist, DefaultArtist},
        {default_album, DefaultAlbum},
        {insert_artist, lists:concat(["INSERT INTO ", TNameArtist, " VALUES ('~s', '~s', ~p);"])},
        {insert_artist_def, lists:concat(["INSERT INTO ", TNameArtist, " VALUES ('~s', '~s');"])},
        {insert_album, lists:concat(["INSERT INTO ", TNameAlbum, " VALUES ('~s', ~p);"])},
        {insert_track, lists:concat(["INSERT INTO ", TNameTrack, " VALUES ('~s', ~p);"])},
        {update_artist, lists:concat(["UPDATE ", TNameArtist, " SET ~s WHERE Name = '~s';"])},
        {update_album, lists:concat(["UPDATE ", TNameAlbum, " SET ~s WHERE Name = '~s';"])},
        {update_track, lists:concat(["UPDATE ", TNameTrack, " SET ~s WHERE Name = '~s';"])},
        {delete_artist, lists:concat(["DELETE FROM ", TNameArtist, " WHERE Name = '~s';"])},
        {delete_album, lists:concat(["DELETE FROM ", TNameAlbum, " WHERE Name = '~s';"])},
        {delete_track, lists:concat(["DELETE FROM ", TNameTrack, " WHERE Name = '~s';"])},
        {select_artist, lists:concat(["SELECT * FROM ", TNameArtist, " WHERE Name = '~s';"])},
        {select_album, lists:concat(["SELECT * FROM ", TNameAlbum, " WHERE Name = '~s';"])},
        {select_track, lists:concat(["SELECT * FROM ", TNameTrack, " WHERE Name = '~s';"])}
    ]).

end_per_suite(Config) ->
    Config.
=======
  aql:start(),
  TNameArtist = "ArtistTest",
  TNameAlbum = "AlbumTest",
  TNameTrack = "TrackTest",
  DefaultArtist = 0,
  DefaultAlbum = false,
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameArtist,
    " (Name VARCHAR PRIMARY KEY, City VARCHAR,",
    " Awards INTEGER DEFAULT ", DefaultArtist, ");"])),
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameAlbum,
    " (Name VARCHAR PRIMARY KEY,",
    " IsSingle BOOLEAN DEFAULT ", DefaultAlbum, ");"])),
  {ok, [], _Tx} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameTrack,
    " (Name VARCHAR PRIMARY KEY, Plays COUNTER_INT CHECK (Plays > 0));"
  ])),
  lists:append(Config, [
    {tname_artist, TNameArtist},
    {tname_album, TNameAlbum},
    {tname_track, TNameTrack},
    {default_artist, DefaultArtist},
    {default_album, DefaultAlbum},
    {insert_artist, lists:concat(["INSERT INTO ", TNameArtist, " VALUES ('~s', '~s', ~p);"])},
    {insert_artist_def, lists:concat(["INSERT INTO ", TNameArtist, " VALUES ('~s', '~s');"])},
    {insert_album, lists:concat(["INSERT INTO ", TNameAlbum, " VALUES ('~s', ~p);"])},
    {insert_track, lists:concat(["INSERT INTO ", TNameTrack, " VALUES ('~s', ~p);"])},
    {update_artist, lists:concat(["UPDATE ", TNameArtist, " SET ~s WHERE Name = '~s';"])},
    {update_album, lists:concat(["UPDATE ", TNameAlbum, " SET ~s WHERE Name = '~s';"])},
    {update_track, lists:concat(["UPDATE ", TNameTrack, " SET ~s WHERE Name = '~s';"])},
    {delete_artist, lists:concat(["DELETE FROM ", TNameArtist, " WHERE Name = '~s';"])},
    {delete_album, lists:concat(["DELETE FROM ", TNameAlbum, " WHERE Name = '~s';"])},
    {delete_track, lists:concat(["DELETE FROM ", TNameTrack, " WHERE Name = '~s';"])},
    {select_artist, lists:concat(["SELECT * FROM ", TNameArtist, " WHERE Name = '~s';"])},
    {select_album, lists:concat(["SELECT * FROM ", TNameAlbum, " WHERE Name = '~s';"])},
    {select_track, lists:concat(["SELECT * FROM ", TNameTrack, " WHERE Name = '~s';"])}
  ]).

end_per_suite(Config) ->
  aql:stop(),
  Config.
>>>>>>> 5c22887a31ac543b25d619f381fa2512e2cc1a59

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() ->
    [
        select_all,
        insert_artist,
        insert_with_default_artist,
        insert_duplicate,
        delete_album,
        delete_non_existent_album,
        commit_transaction,
        abort_transaction,
        error_transaction
    ].

select_all(_Config) ->
    TNameEmpty = "EmptyTableTest",
    TNameFull = "FullTableTest",
    {ok, [], _Tx} = tutils:create_single_table(TNameEmpty, "AW"),
    {ok, [], _Tx} = tutils:create_single_table(TNameFull, "AW"),
    {ok, [[]], _Tx} = tutils:select_all(TNameEmpty),
    {ok, [], _Tx} = tutils:insert_single(TNameFull, 1),
    {ok, [], _Tx} = tutils:insert_single(TNameFull, 2),
    {ok, [], _Tx} = tutils:insert_single(TNameFull, 3),
    {ok, [Res], _Tx} = tutils:select_all(TNameFull),
    ?assertEqual(3, length(Res)).

insert_artist(Config) ->
    TName = ?value(tname_artist, Config),
    Artist = "Sam",
    City = "NY",
    Awards = 1,
    {ok, [], _Tx} = tutils:aql(?format(insert_artist, [Artist, City, Awards], Config)),
    SearchKey = lists:concat(["'", Artist, "'"]),
    [Artist, City, Awards] = tutils:read_keys(TName, "Name", SearchKey, ["Name", "City", "Awards"]).

insert_with_default_artist(Config) ->
    TName = ?value(tname_artist, Config),
    Artist = "Mike",
    City = "LS",
    Awards = ?value(default_artist, Config),
    {ok, [], _Tx} = tutils:aql(?format(insert_artist_def, [Artist, City], Config)),
    SearchKey = lists:concat(["'", Artist, "'"]),
    [Artist, City, Awards] = tutils:read_keys(TName, "Name", SearchKey, ["Name", "City", "Awards"]).

insert_duplicate(Config) ->
    TName = ?value(tname_artist, Config),
    Artist = "Sam",
    City = "NY",
    Awards = 1,
    Query = ?format(insert_artist, [Artist, City, Awards], Config),
    {ok, [], _Tx} = tutils:aql(lists:concat([Query, Query])),
    SearchKey = lists:concat(["'", Artist, "'"]),
    [Artist, City, Awards] = tutils:read_keys(TName, "Name", SearchKey, ["Name", "City", "Awards"]).

delete_album(Config) ->
    TName = ?value(tname_album, Config),
    Album = "Hello",
    {ok, [], _Tx} = tutils:aql(?format(insert_album, [Album, true], Config)),
    {ok, [], _Tx} = tutils:aql(?format(delete_album, [Album], Config)),
    tutils:assertState(false, list_to_atom(TName), Album).

delete_non_existent_album(Config) ->
    TName = ?value(tname_album, Config),
    Album = "IDontExist",
    {ok, [], _Tx} = tutils:aql(?format(delete_album, [Album], Config)),
    tutils:assertState(false, list_to_atom(TName), Album).

commit_transaction(Config) ->
    TNameArtist = ?value(tname_artist, Config),
    Artist = "Rob",
    City = "FL",
    Awards = 3,
    {ok, [{ok, {begin_tx, Tx}}], Tx} = tutils:aql("BEGIN TRANSACTION"),
    {ok, [], Tx} = tutils:aql(?format(insert_artist, [Artist, City, Awards], Config), Tx),
    {ok, [{ok, commit_tx}], _} = tutils:aql("COMMIT TRANSACTION", Tx),

    SearchKey = lists:concat(["'", Artist, "'"]),
    [Artist, City, Awards] = tutils:read_keys(TNameArtist, "Name", SearchKey, ["Name", "City", "Awards"]).

abort_transaction(Config) ->
    TNameArtist = ?value(tname_artist, Config),
    Artist = "Jon",
    City = "CO",
    Awards = 5,
    {ok, [{ok, {begin_tx, Tx}}], Tx} = tutils:aql("BEGIN TRANSACTION"),
    {ok, [], Tx} = tutils:aql(?format(insert_artist, [Artist, City, Awards], Config), Tx),
    {ok, [{ok, abort_tx}], _} = tutils:aql("ABORT TRANSACTION", Tx),

    SearchKey = lists:concat(["'", Artist, "'"]),
    [] = tutils:read_keys(TNameArtist, "Name", SearchKey, ["Name", "City", "Awards"]).

error_transaction(Config) ->
    TNameArtist = ?value(tname_artist, Config),
    TNameAlbum = ?value(tname_album, Config),
    DefaultAlbum = false,

    {ok, [{ok, {begin_tx, Tx}}], Tx} = tutils:aql("BEGIN TRANSACTION"),

<<<<<<< HEAD
    {ok, [{error, _}], _} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameAlbum,
        " (Name VARCHAR PRIMARY KEY,",
        " IsSingle BOOLEAN DEFAULT ", DefaultAlbum, ",",
        " Art INT FOREIGN KEY UPDATE-WINS REFERENCES ",
        TNameArtist, "(Awards));"]), Tx).
=======
  {_, [{error, _}], _} = tutils:aql(lists:concat(["CREATE AW TABLE ", TNameAlbum,
    " (Name VARCHAR PRIMARY KEY,",
    " IsSingle BOOLEAN DEFAULT ", DefaultAlbum, ",",
    " Art INT FOREIGN KEY UPDATE-WINS REFERENCES ",
    TNameArtist, "(Awards));"]), Tx).
>>>>>>> 5c22887a31ac543b25d619f381fa2512e2cc1a59

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Apr 2015 12:15 AM
%%%-------------------------------------------------------------------
-module(mqtt_session_repo).
-include("gen_repo.erl").
-include("mqtt_session.hrl").
-author("Kalin").

-behaviour(gen_repo).

%% API
-export([wait_for_tables/0, create_tables/2, save/1, load/1]).

-ifdef(TEST).
    -export([clear_tables/0, delete_tables/0]).
-endif.

-define(SESSION_RECORD, session_out).


save(Session) ->
    Fun = fun() -> mnesia:write(Session) end,
    mnesia_do(Fun).

load(ClientId) ->
    case mnesia:dirty_read(?SESSION_RECORD,ClientId) of
        [Record] -> Record;
        []       -> {error,not_found}
    end.


mnesia_do(Fun) ->
    Fun().

%%-------------------------------------------------------------------
%% Table initialization/startup
%%-------------------------------------------------------------------

wait_for_tables() ->
    mnesia:wait_for_tables(?SESSION_RECORD,30000).

create_tables([],NFragments) ->
    create_tables([node()],NFragments);

create_tables(Nodes,NFragments) ->
    DefaultProps = ?BASIC_TABLE_DEF(Nodes,set,?SESSION_RECORD),
    Props = if NFragments < 2 ->
        DefaultProps;
                true ->
                    [?DISTRIBUTED_DEF(NFragments,Nodes) | DefaultProps]
            end,

    case mnesia:create_table(?SESSION_RECORD, Props) of
        {atomic, ok}                                 -> ok;
        {aborted, {already_exists, ?SESSION_RECORD}} -> ok
    end.



-ifdef(TEST).
clear_tables() ->
    mnesia:clear_table(?SESSION_RECORD).

delete_tables() ->
    mnesia:delete_table(?SESSION_RECORD).
-endif.
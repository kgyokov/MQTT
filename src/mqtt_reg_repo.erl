%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%% Registers a Pid so that it handles messages for a particular ClientId
%%% Only one Pid may be registered per client. If there is an existing one then we overwite it.
%%% Two things can happen then:
%%% - The OldPid is a dead process. We are done.
%%% - The OldPid is still alive and needs to be told to terminate. In that case
%%%
%%%
%%%
%%%
%%% @end
%%% Created : 08. Dec 2014 10:28 PM
%%%-------------------------------------------------------------------
-module(mqtt_reg_repo).
-author("Kalin").

%% API
-export([register/2, unregister/2, get_registration/1, create_tables/2, wait_for_tables/0, register/1]).


-record(client_reg, {client_id, connection_pid, timestamp}).

-ifdef(TEST).
    -export([clear_tables/0,delete_tables/0]).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-define(REG_TABLE, client_reg).

-define(BASIC_TABLE_DEF(Nodes),[
    {?PERSISTENCE, Nodes},
    {type,set},
    {attributes,record_info(fields,client_reg)}
]).

-define(DISTRIBUTED_DEF(NFragments,Nodes), (
    {frag_properties,
        {n_fragments,NFragments},
        {node_pool,Nodes}}
)).


%% @doc
%%
%% Creates the mnesia tables. To be called only once.
%%
%% @end

create_tables([],NFragments) ->
    create_tables([node()],NFragments);

create_tables(Nodes,NFragments) ->
    DefaultProps = ?BASIC_TABLE_DEF(Nodes),
    Props = if NFragments < 2 ->
        DefaultProps;
                true ->
                    [?DISTRIBUTED_DEF(NFragments,Nodes) | DefaultProps]
            end,

    case mnesia:create_table(?REG_TABLE, Props) of
        {atomic, ok}                            -> ok;
        {aborted, {already_exists, ?REG_TABLE}} -> ok
    end.

wait_for_tables()->
    mnesia:wait_for_tables(?REG_TABLE,5000).




%%
%% @doc
%%
%% Registers a Process as responsible for the connection to a given ClientId. Only one Process can be registered
%% per client
%%
%% @end

register(Pid, ClientId)->
    NewReg = #client_reg{client_id = ClientId,connection_pid = Pid,timestamp = time()},
    F = fun()->
        %% take write lock
        case mnesia:read(?REG_TABLE, ClientId, write) of
            [] ->
                %error_logger:info_msg("empty result"),
                P = mnesia:write(?REG_TABLE,NewReg,write),
                %error_logger:info_msg(P),
                ok;
            [E = #client_reg{connection_pid = EPid}] ->
                case EPid of
                    Pid ->
                        ok;
                    undefined ->
                        ok = mnesia:write(?REG_TABLE,NewReg,write),
                        ok;
                    _ ->
                        ok = mnesia:write(?REG_TABLE,NewReg,write),
                        {dup_detected, EPid}
                end
        end
    end,

    case mnesia:activity(transaction,F,[],mnesia_frag) of
        ok ->
            ok;
        {dup_detected,EPid} ->
            handle_duplicate(EPid),
            {dup_detected,EPid}
    end.

register(ClientId) ->
    ?MODULE:register(ClientId,self()).

%%
%% @doc
%%
%% Unregisters a Process responsible for the connection to a given <b>ClientId</b>. Sets the <b>Pid</b> to <i>undefined</i>
%%
%% @end
unregister(Pid,ClientId)->
    F = fun()->
        %% take write lock
        case mnesia:read(?REG_TABLE, ClientId, write) of
            [] ->
                ok;
            [E = #client_reg{connection_pid = EPid}] when EPid =:= Pid ->
                mnesia:write(?REG_TABLE, E#client_reg{connection_pid = undefined}, write),
                ok;
            [#client_reg{connection_pid = EPid}] when EPid =/= Pid ->
                ok
        end
    end,
    mnesia:activity(transaction,F,[],mnesia_frag).

get_registration(ClientId)->
    Fun = fun() ->
        case mnesia:dirty_read(?REG_TABLE, ClientId) of
            [] ->
                undefined;
            [#client_reg{client_id = ClientId,connection_pid = ConnPid}] ->
                case ConnPid of
                    undefined -> undefined;
                    _ ->        {ok,ConnPid}
                end
        end
    end,
    mnesia:activity(transaction,Fun,[],mnesia_frag).


handle_duplicate(Pid)->
    mqtt_connection:close_duplicate(Pid).


%% ------------------------------------------------------------
%% Mnesia Tests
%% @doc
%%
%% Creates the mnesia tables. To be called only once.
%%
%% @end
%% ------------------------------------------------------------


-ifdef(TEST).

clear_tables() ->
    {atomic,ok} = mnesia:clear_table(?REG_TABLE).

delete_tables() ->
    {atomic,ok} = mnesia:delete_table(?REG_TABLE).

-endif.


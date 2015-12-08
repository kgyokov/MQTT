%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% Simple document based implementation of subscriptions
%%% @end
%%% Created : 09. Dec 2014 2:13 AM
%%%-------------------------------------------------------------------
-module(mqtt_sub_repo).
-author("Kalin").

-include_lib("mqtt_internal_msgs.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_tables/2, wait_for_tables/0,
    save_sub/2, remove_sub/2,
    get_matches/1, get_matching_subs/1,
    clear/1, load/1, get_sub/1]).

-ifdef(TEST).
    -export([clear_tables/0,delete_tables/0]).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-define(SUB_RECORD, mqtt_sub).

-record(mqtt_sub, {
    filter  ::binary(),
    subs    ::any(),
    pid     ::pid(),
    seq = 1 ::integer()
}).


-define(BASIC_TABLE_DEF(Nodes),[
    {?PERSISTENCE, Nodes},
    {type,set},
    {attributes,record_info(fields,?SUB_RECORD)}
]).

-define(DISTRIBUTED_DEF(NFragments,Nodes), (
    {frag_properties,
        {n_fragments,NFragments},
        {node_pool,Nodes}}
)).


%% ======================================================================
%% API
%% ======================================================================


%% @doc
%% Appends a new subscription OR replaces an existing one with a new QoS
%% @end

-spec save_sub(ClientId::client_id(),
    {
        Filter::binary(),
        QoS::qos(),
        Seq::integer(),
        ClientPid::pid()}
) ->
    {ok,Result::any()}.

save_sub(Filter,{ClientId,QoS,Seq,ClientPid}) ->
    ClientReg = {QoS,Seq,ClientPid},
    Fun =
        fun() ->
            R =
                case mnesia:read(?SUB_RECORD, Filter, write) of
                    [] ->   new(Filter);
                    [S]->   S
                end,
            #mqtt_sub{subs = Subs} = R,
            case orddict:find(ClientId,Subs) of
                {ok, ClientReg} ->
                    {ok,existing};
                _  ->
                    persist_sub(R,ClientId,ClientReg),
                    {ok,new}
            end
        end,
    mnesia_do(Fun).

persist_sub(R =  #mqtt_sub{subs = Subs},ClientId,ClientReg) ->
    mnesia:write(R#mqtt_sub{subs = orddict:store(ClientId,ClientReg,Subs)}).


%% @doc
%% Removes a subscription
%%
%% @end

-spec remove_sub(Filter::binary(),ClientId::client_id()) -> ok.

remove_sub(Filter,ClientId) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_RECORD,Filter,write) of
                [] ->
                    ok;
                [S = #mqtt_sub{subs = Subs}] ->
                    case orddict:is_key(ClientId,Subs) of
                        true ->
                            ok = mnesia:write(S#mqtt_sub{subs = orddict:erase(ClientId,Subs)});
                        false ->
                            ok
                    end
            end
        end,
    mnesia_do(Fun).



%% @doc
%% Locks a given Filter to a process and loads a list of subscriptions for that filter
%% @end

-spec load(Filter::binary()) -> [{ClientId::client_id(),QoS::qos(),Seq::integer(),ClientPid::pid()}].
load(Filter) ->
    load(self(),Filter).

-spec load(Pid::pid,Filter::binary()) -> [{ClientId::client_id(),QoS::qos(),Seq::integer(),ClientPid::pid()}].
load(Pid,Filter) ->
    Fun =
        fun() ->
            R =
                case mnesia:read(?SUB_RECORD, Filter, write) of
                    [] ->   new(Filter);
                    [S]->   S
                end,
            NewSeq = R#mqtt_sub.seq + 1,
            mnesia:write(R#mqtt_sub{pid = Pid,seq = NewSeq}),
            R#mqtt_sub.subs
        end,
    Subs = mnesia_do(Fun),
    [{ClientId,QoS,Seq,ClientPid} || {ClientId,{QoS,Seq,ClientPid}} <- orddict:to_list(Subs)].

%% @doc
%% Gets a list of ALL subscriptions matching a topic, selecting the one with the highest QoS
%% per client
%%
%% @end

-spec get_matches(Topic::topic()) -> [{ClientId::client_id(),QoS::qos(),ClientPid::pid()}].

get_matches(Topic) ->
    Patterns = mqtt_topic:explode(Topic),
    Spec = [{
                 #mqtt_sub{filter = '$1', subs = '$2', _ = '_'},
                 [{'=:=', '$1', P}],
                 ['$2']
             } || P <- Patterns],
    Rs = mnesia:dirty_select(?SUB_RECORD, Spec),
    AllSubs = lists:flatmap(fun(Subs) -> orddict:to_list(Subs) end, Rs),
    Merged = lists:foldr(
        fun({ClientId,QoS},Acc) ->
            case orddict:find(ClientId,Acc) of
                {ok,OldQoS} when QoS =< OldQoS ->
                    Acc;
                _ ->
                    orddict:store(ClientId,QoS,Acc)
            end
        end,
        orddict:new(), AllSubs),
    orddict:to_list(Merged).


%% @doc
%% Gets the Pids of the Subs matching the given topic
%% @end
-spec get_matching_subs(Topic::topic()) -> [{Filter::binary(),Pid::pid()}].

get_matching_subs(Topic) ->
    Patterns = mqtt_topic:explode(Topic),
    Spec = [{
        #mqtt_sub{filter = '$1', pid = '$2', _ = '_'},
        [{'=:=', '$1', P}],
        [['$1','$2']]
    } || P <- Patterns],
    [{F,P}|| [F,P]<- mnesia:dirty_select(?SUB_RECORD, Spec)].

clear(Filter) ->
    claim(Filter,undefined).

claim(Filter,Pid) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_RECORD,Filter,write) of
                [] ->
                    ok = mnesia:write(#mqtt_sub{filter = Filter,pid = Pid});
                [S = #mqtt_sub{pid = OldPid}] when OldPid =/= Pid ->
                    ok = mnesia:write(S#mqtt_sub{pid = Pid});
                _ -> ok
            end
        end,
    mnesia_do(Fun).

get_sub(Filter) ->
    Spec = [{
        #mqtt_sub{filter = '$1', pid = '$2', _ = '_'},
        [{'=:=', '$1', Filter}],
        ['$2']
    }],
    case mnesia:dirty_select(?SUB_RECORD, Spec) of
        [] -> error;
        [Pid] -> {ok,Pid}
    end.


%% ======================================================================
%% PRIVATE Misc
%% ======================================================================

mnesia_do(Fun) ->
    mnesia:activity(transaction,Fun,[],mnesia_frag).

new(Filter) ->
    #mqtt_sub{filter = Filter, subs = orddict:new()}.


%% ======================================================================
%% Table creation, cleanup, etc.
%% ======================================================================

-ifdef(TEST).

clear_tables() ->
    {atomic,ok} = mnesia:clear_table(?SUB_RECORD).

delete_tables() ->
    {atomic,ok} = mnesia:delete_table(?SUB_RECORD).

-endif.

wait_for_tables() ->
    ok = mnesia:wait_for_tables([?SUB_RECORD],5000).

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

    case mnesia:create_table(?SUB_RECORD, Props) of
        {atomic, ok}                            -> ok;
        {aborted, {already_exists, ?SUB_RECORD}} -> ok
    end.
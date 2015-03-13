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

-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_tables/2, add_sub/3, remove_sub/2, get_all/1, wait_for_tables/0]).

-ifdef(TEST).
    -export([clear_tables/0,delete_tables/0]).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-record(mqtt_sub, {topic, subs}).


-define(SUB_TABLE, mqtt_sub).

-define(BASIC_TABLE_DEF(Nodes),[
    {?PERSISTENCE, Nodes},
    {type,set},
    {attributes,record_info(fields,mqtt_sub)}
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
    {atomic, ok} = mnesia:create_table(?SUB_TABLE, Props).

%% @doc
%% Appends a new subscription OR replaces an existing one with a new QoS
%%
%% @end

add_sub(ClientId, Topic, QoS) ->
    Fun =
        fun() ->
            R =
                case mnesia:read(?SUB_TABLE, Topic, write) of
                    [] ->
                        new(Topic);
                    [S]->
                        S
                end,
            #mqtt_sub{subs = Subs} = R,
            case orddict:find(ClientId,Subs) of
                {ok, QoS} ->
                    ok;
                T  ->
                    append_sub(R,ClientId,QoS)
            end
        end,
    ok = mnesia:activity(transaction,Fun).

append_sub(R =  #mqtt_sub{subs = Subs}, ClientId,QoS) ->
    mnesia:write(R#mqtt_sub{subs = orddict:store(ClientId,QoS,Subs)}).


%% @doc
%% Removes a subscription
%%
%% @end
remove_sub(ClientId, Topic) ->
    Fun =
        fun() ->
            case mnesia:read({?SUB_TABLE, Topic}) of
                [] ->
                    ok;
                [S = #mqtt_sub{subs = Subs}] ->
                    case orddict:is_key(ClientId,Subs) of
                        true ->
                            mnesia:write(S#mqtt_sub{subs = orddict:erase(ClientId,Subs)});
                        false -> ok
                    end
            end
        end,
    ok = mnesia:activity(transaction,Fun).


%% @doc
%% Gets a list of ALL subscriptions matching a topic, selecting the one with the highest QoS
%% per client
%%
%% @end
get_all(Topic) ->
    Patterns = mqtt_topic:explode(Topic),
    Spec = [{{'_',P},[],['$_']} || P <- Patterns],
    Fun =
        fun() ->
            %% Rs = mnesia:dirty_select(?SUB_TABLE, Spec),
            Rs = lists:flatten([ mnesia:read({?SUB_TABLE,Pattern}) || Pattern <- Patterns]),
            AllSubs = lists:flatmap(fun(#mqtt_sub{subs = Subs}) -> orddict:to_list(Subs) end, Rs),
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
            orddict:to_list(Merged)
        end,
    mnesia:activity(transaction,Fun).
    %%mnesia:async_dirty(Fun).


wait_for_tables()->
    mnesia:wait_for_tables(?SUB_TABLE,20000).

new(Topic) ->
    #mqtt_sub{topic = Topic, subs = orddict:new()}.


-ifdef(TEST).

clear_tables() ->
    {atomic,ok} = mnesia:clear_table(?SUB_TABLE)
.

delete_tables() ->
    {atomic,ok} = mnesia:delete_table(?SUB_TABLE).

-endif.
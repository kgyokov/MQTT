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

%% API
-export([create_tables/2, add_sub/2, remove_sub/2, get_all/1]).

-record(mqtt_sub, {topic, clients}).


-define(SUB_TABLE, mqtt_sub).

-define(TABLE_DEF,[
    {type,set},
    {attributes,record_info(fields,mqtt_sub)}
]).

%% @doc
%%
%% Creates the mnesia tables. To be called only once.
%%
%% @end
create_tables(Nodes,NFragments) ->
    mnesia:create_schema(Nodes),
    mnesia:create_table(?SUB_TABLE, [
        {disc_copies, Nodes},
        {frag_properties,
            {n_fragments,NFragments},
            {node_pool,Nodes}
        }
        |?TABLE_DEF]
    ).


add_sub(ClientId, Topic)->
    Fun =
        fun() ->
            Subs =
                case mnesia:read(?SUB_TABLE,Topic, write) of
                    [] ->
                        new(Topic);
                    [S]->
                        S
                end,
            #mqtt_sub{clients = Clients} = Subs,
            case gb_sets:is_member(ClientId,Clients) of
                false ->
                    mnesia:write(Subs#mqtt_sub{clients = gb_sets:add(ClientId,Clients)});
                true -> ok
            end
        end,
    mnesia:activity(transaction,Fun).

remove_sub(ClientId, Topic) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_TABLE,Topic, write) of
                [] ->
                    ok;
                [S = #mqtt_sub{clients = Clients}]->
                    case gb_sets:is_member(ClientId,Clients) of
                        true ->
                            mnesia:write(S#mqtt_sub{clients = gb_sets:delete(ClientId,Clients)});
                        false -> ok
                    end
            end
        end,
    mnesia:activity(transaction,Fun).

get_all(Topic) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_TABLE,Topic) of
                [] ->
                    [];
                [#mqtt_sub{clients = Clients}] ->
                    gb_sets:to_list(Clients)
            end
        end,
    mnesia:async_dirty(Fun).


new(Topic) ->
    #mqtt_sub{topic = Topic,clients = gb_sets:new()}.

publish_to_all(Message,Topic,Pids) ->
    [
        mqtt_session_out:publish(Message,Topic,Pid) ||
        Pid	<- Pids
    ],
    ok.
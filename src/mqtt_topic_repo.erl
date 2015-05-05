%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Apr 2015 1:10 AM
%%%-------------------------------------------------------------------
-module(mqtt_topic_repo).
-author("Kalin").

-include("mqtt_internal_msgs.hrl").


-export([enqueue/2, get_retained/1, new/1]).

-record(queue_msg,{
    retained    ::boolean(),
    content     ::binary(),
    qos         ::qos(),
    client_seq  ::non_neg_integer(),
    topic_seq   ::non_neg_integer()
}).

-record(mqtt_topic,{
    topic       ::binary(),
    retained    ::#queue_msg{},
    seq = 0     ::non_neg_integer()
    %% queue       ::term()
}).

-export([create_tables/2, wait_for_tables/0]).

-define(TOPIC_RECORD, mqtt_topic).

-ifdef(TEST).
    -export([clear_tables/0,delete_tables/0]).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-define(BASIC_TABLE_DEF(Nodes),[
    {?PERSISTENCE, Nodes},
    {type,set},
    {attributes,record_info(fields,?TOPIC_RECORD)}
]).

-define(DISTRIBUTED_DEF(NFragments,Nodes), (
    {frag_properties,
     {n_fragments,NFragments},
     {node_pool,Nodes}}
)).

-ifdef(TEST).
clear_tables() ->
    {atomic,ok} = mnesia:clear_table(?TOPIC_RECORD).

delete_tables() ->
    {atomic,ok} = mnesia:delete_table(?TOPIC_RECORD).
-endif.

%% ====================================================================
%%  API
%% ====================================================================
new(Topic) ->
    #mqtt_topic{topic = Topic}.


enqueue(Topic, Msg) ->
    Fun =
    fun() ->
        R =
            case mnesia:read(?TOPIC_RECORD, Topic, write) of
                [] ->   new(Topic);
                [S]->   S
            end,
        do_enqueue(R,Msg),
        mnesia:write(R)
    end,
    mnesia_do(Fun).

get_retained(Topics) ->
    Ms = [{
              #mqtt_topic{topic = '$1', retained = '$2', _ = '_'},
              [{'=:=','$1',Topic}],
              ['$2']
          }|| Topic <- Topics],
    mnesia:dirty_select(?TOPIC_RECORD,Ms).

%% ===========================================================================
%% Internal DB functions
%% ===========================================================================

mnesia_do(Fun) ->
    mnesia:activity(transaction,Fun,[],mnesia_frag).

%% ===========================================================================
%% Internal pure functions
%% ===========================================================================

do_enqueue(Rec = #mqtt_topic{%%queue = _Q,
                             seq  = Seq,
                             retained = Ret},
           #mqtt_message{retain = Retain,
                         content = Content,
                         qos = QoS,
                         seq = ClientSeq}) ->

    NewSeq = Seq+1,
    NewMsg = #queue_msg{qos = QoS,
                        content = Content,
                        client_seq = ClientSeq,
                        topic_seq = NewSeq},
    Rec#mqtt_topic{retained = case Retain of
                                  true -> NewMsg;
                                  false -> Ret
                              end,
                   %% @todo: add to the queue
                   %% queue = queue:in(NewMsg,Q),
                   seq = NewSeq}.

%% do_get_retained(#mqtt_topic{retained = Retained}) ->
%%     Retained.

%% ========================================================================
%% Table initialization/startup functions
%% ========================================================================

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

    case mnesia:create_table(?TOPIC_RECORD, Props) of
        {atomic, ok}                            -> ok;
        {aborted, {already_exists, ?TOPIC_RECORD}} -> ok
    end.

wait_for_tables()->
    mnesia:wait_for_tables(?TOPIC_RECORD,5000).




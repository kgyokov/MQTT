%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Topic repository - persists messages published for a particular topic
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
    topic,          %%::binary(),
    retained,       %%::#queue_msg{},
    seq = 0         %%::non_neg_integer()
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
            mqtt_filter_index:add_topic(Topic),
            R1 = do_enqueue(R,Msg),
            mnesia:write(R1)
        end,
    mnesia_do(Fun),
    error_logger:info_msg("Enqueued ~p for topic ~p",[Msg,Topic]).

get_retained(Filters) ->
    Topics = mqtt_filter_index:get_matching_topics(Filters),
    error_logger:info_msg("Matching Topics for ~p~n",[Topics]),
    Ms = [{
              #mqtt_topic{topic = '$1', retained = '$2', _ = '_'},
              [{'=:=','$1',Topic}],
              [['$1','$2']]
          }|| Topic <- Topics],
    RetainedMsgs = mnesia:dirty_select(?TOPIC_RECORD,Ms),
    error_logger:info_msg("Read msgs ~p",[RetainedMsgs]),
    [{Topic,Content,ClientSeq,QoS}
     || [Topic, #queue_msg{qos = QoS,
                           content = Content,
                           client_seq = ClientSeq,
                           topic_seq = _NewSeq}] <- RetainedMsgs].

%% ===========================================================================
%% Internal DB functions
%% ===========================================================================

mnesia_do(Fun) ->
    mnesia:activity(transaction,Fun,[],mnesia_frag).

%% ===========================================================================
%% Internal pure functions
%% ===========================================================================

do_enqueue(Rec,M) ->
    maybe_retain(Rec,M). %%@todo: actually enqueue

maybe_retain(Rec,#mqtt_message{retain = false}) ->
    Rec;

maybe_retain(Rec,#mqtt_message{content = <<>>}) ->
    Rec#mqtt_topic{retained = undefined};

maybe_retain(Rec,#mqtt_message{content = Content,
                               qos = QoS,
                               seq = ClientSeq}) ->
    #mqtt_topic{seq = Seq} = Rec,
    NewRetained = #queue_msg{qos = QoS,
                             content = Content,
                             client_seq = ClientSeq,
                             topic_seq = Seq + 1},
    Rec#mqtt_topic{retained = NewRetained}.

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
        {atomic, ok}                               -> ok;
        {aborted, {already_exists, ?TOPIC_RECORD}} -> ok
    end.

wait_for_tables()->
    ok = mnesia:wait_for_tables([?TOPIC_RECORD],5000).




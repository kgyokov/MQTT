%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Purely functional outgoing Session data structure.
%%% Does not have any side effects (e.g. message sending or persistence)
%%% @end
%%% Created : 21. Feb 2015 5:37 PM
%%%-------------------------------------------------------------------
-module(mqtt_session).
-author("Kalin").

%%-include("mqtt_packets.hrl").
-include("mqtt_internal_msgs.hrl").
-include("mqtt_session.hrl").
%% API
-export([push_msg/2,
         push_msg_comp/2,
         subscribe/2,
         unsubscribe/2,
         get_subs/1,
         message_ack/2,
         message_pub_rec/2,
         message_pub_comp/2,
         msg_in_flight/1,
         new/0,
         to_publish/4,
         to_pubrel/1,
         append_retained/2]).

-define(DEFAULT_MAX_WINDOW,20).


-record(outgoing,{
    window_size               ::non_neg_integer(),   %% the max number of msgs in flight
    buffer                    ::non_neg_integer(),   %% the min available window size before we request more packages
    waiting_subs              ::[pid()],             %% waiting subs with more work to do
    packet_seq                ::non_neg_integer(),   %% The latest client-assigned packet id
    %% Incremented by 1 for every packet,
    %% (though that is not required by the protocol)
    qos1,                      %% Unacknowledged QoS1 messages
    qos2,                      %% Unacknowledged QoS2 messages
    qos2_rec,
    refs,                     %% Message in transit
    subs                      %% Subscriptions
}).

%% ================================================================================
%% SUBSCRIPTIONS
%% ================================================================================

get_subs(#outgoing{subs = Subs}) ->
    orddict:to_list(Subs).

%% @doc
%% Adds new subscriptions to the session data
%% @end
subscribe(NewSubs,S = #outgoing{subs = Subs}) ->
    %% @todo: Deduplicate
    Subs1 = lists:foldl(fun add_sub/2,Subs,NewSubs),
    S#outgoing{subs = Subs1}.

add_sub({Filter,QoS,Seq},Subs) ->
    orddict:store(Filter,{QoS,Seq},Subs).

%% @doc
%% Removed existing subscriptions from the session data
%% @end
unsubscribe(OldSubs,S = #outgoing{subs = Subs}) ->
    Subs1= lists:foldl(fun orddict:erase/2,Subs, OldSubs),
    S#outgoing{subs = Subs1}.


%% =========================================================================
%% MESSAGES
%% =========================================================================

-type tcr_packet() :: {topic(),content(),boolean(),any()}.

-spec push_msg(#packet{},#outgoing{}) ->
    duplicate | {full,#outgoing{}} | {ok,#'PUBLISH'{},#outgoing{}}.

%% @doc
%% Appends message for delivery
%% @end
push_msg(Packet = #packet{topic = Topic,
                            ref = Ref,
                            qos = MsgQoS},
            SO = #outgoing{subs = Subs,
                           refs = Refs}) ->
    case is_duplicate(Ref,Refs) of
        false ->
            {ok,{_,SubQos}} = mqtt_topic:best_match(Subs,Topic),
            ActualQoS = min(MsgQoS,SubQos),
            store_msg(Packet#packet{qos = ActualQoS},SO);
        true  -> duplicate
    end.

store_msg(Packet = #packet{qos =?QOS_0},SO) ->
    Pub = to_publish(Packet,?QOS_0,undefined,false),
    {ok,Pub,SO};

store_msg(Packet,SO = #outgoing{window_size = 0}) ->
    #outgoing{waiting_subs = Waiting,
              packet_seq = PSeq} = SO,
    #packet{ref = Ref} = Packet,
    Waiting1 = add_to_wait_list(Ref,PSeq,Waiting),
    {full,SO#outgoing{waiting_subs = Waiting1}};

store_msg(Packet = #packet{qos = QoS},SO) when QoS =:= ?QOS_1;
                                               QoS =:= ?QOS_2  ->
    #outgoing{packet_seq = PSeq,
              refs = Refs,
              window_size = WSize} = SO,
    #packet{ref = Ref} = Packet,
    PSeq1 = PSeq+1,
    SO1 = add_to_queue(PSeq1,Packet,SO),
    SO2 = SO1#outgoing{refs = add_ref(Ref,Refs),
                       packet_seq = PSeq1,
                       window_size = WSize -1},
    Pub = to_publish(Packet,QoS,PSeq1,false),
    {ok,Pub,SO2}.

add_to_wait_list(Ref,PSeq,Waiting) ->
    min_val_tree:store(Ref,PSeq,Waiting).


is_duplicate(Ref,Refs) ->
    gb_sets:is_member(Ref,Refs).

add_ref(Ref,Refs) ->
    gb_sets:add(Ref,Refs).

append_retained(Msgs,SO) ->
    append_msg_many(Msgs,true,SO).

append_msg_many(Msgs,Retain,SO) -> append_msg_many(Msgs,Retain,[],SO).

append_msg_many([],_Retain,Results,SOAcc) -> {Results,SOAcc};

append_msg_many([CTRPacket|T],Retain,Results,SOAcc) ->
    case push_msg(CTRPacket,SOAcc) of
        full -> {Results,SOAcc};
        Result -> append_msg_many(T,Retain,[Result|Results],SOAcc)
    end.

add_to_queue(PSeq,CTRPacket = #packet{qos = ?QOS_1},SO) ->
    #outgoing{qos1 = QosQueue} = SO,
    SO#outgoing{qos1 = orddict:store(PSeq,CTRPacket,QosQueue)};

add_to_queue(PSeq,CTRPacket = #packet{qos = ?QOS_2},SO) ->
    #outgoing{qos2 = QosQueue} = SO,
    SO#outgoing{qos2 = orddict:store(PSeq,CTRPacket,QosQueue)}.

push_msg_comp(Ref,SO = #outgoing{refs = Refs}) ->
    SO#outgoing{refs = gb_sets:del_element(Ref,Refs)}.

%% ==================================================================
%% Message Acknowledgements
%% ==================================================================

-spec message_ack(packet_id(),#outgoing{}) ->
    duplicate | {ok,[{WSize::integer(),Filter::binary()}],#outgoing{}}.

message_ack(PacketId,SO = #outgoing{packet_seq = PSeq,
                                    qos1 = QoS1Msgs,
                                    window_size = WSize,
                                    waiting_subs = Waiting}) ->
    AckSeq = get_ack_seq(PacketId,PSeq),
    case orddict:find(AckSeq,QoS1Msgs) of
        {ok,_} ->
            WSize1 = WSize+1,
            {Notifs, Waiting1} = subs_to_notify(WSize1,Waiting),
            SO1 = SO#outgoing{qos1 = orddict:erase(AckSeq,QoS1Msgs),
                              waiting_subs = Waiting1,
                              window_size = WSize1},
            {ok,Notifs,SO1};
        error ->
            duplicate
    end.


message_pub_rec(PacketId,SO) ->
    #outgoing{qos2 = Msgs,
              qos2_rec = Ack,
              packet_seq = PSeq} = SO,
    AckSeq = get_ack_seq(PacketId,PSeq),
    case orddict:find(AckSeq,Msgs) of
        {ok,_} ->
            {ok,
                SO#outgoing{qos2 = orddict:erase(AckSeq,Msgs),
                               qos2_rec = ordsets:add_element(AckSeq,Ack)}
            };
        error ->
            duplicate
    end.


-spec message_pub_comp(packet_id(),#outgoing{}) ->
    duplicate | {ok,[{WSize::integer(),Filter::binary()}],#outgoing{}}.

message_pub_comp(PacketId,SO = #outgoing{packet_seq = PSeq,
                                         qos2_rec = Ack,
                                         window_size = WSize,
                                         waiting_subs = Waiting}) ->
    AckSeq = get_ack_seq(PacketId,PSeq),
    case ordsets:is_element(AckSeq,Ack) of
        true ->
            WSize1 = WSize+1,
            {Notifs,Waiting1} = subs_to_notify(WSize1,Waiting),
            SO1 = SO#outgoing{qos2_rec = ordsets:del_element(AckSeq,Ack),
                              waiting_subs = Waiting1,
                              window_size = WSize1},
            {ok,Notifs,SO1};
        false ->
            duplicate
    end.

%% @doc
%% Determines which Subscriptions to notify about and open window,
%% and what window size to indicate to each subscription (currently hardcoded to 1)
%% @end
subs_to_notify(Wnd,Waiting) ->
    {Refs,WSubs1} = min_val_tree:split(Wnd,Waiting),
    Notifs = [{1,Ref}|| Ref <- Refs],
    {Notifs,WSubs1}.


%% =========================================================================
%% RECOVERY
%% =========================================================================

%% [MQTT-4.4.0-1] "When a Client reconnects with CleanSession set to 0, both the Client and Server MUST
%% re-send any unacknowledged PUBLISH Packets (where QoS > 0) and PUBREL Packets using their original
%% Packet Identifiers"
msg_in_flight(#outgoing{qos1 = UnAck1,
                           qos2 = UnAck2,
                           qos2_rec = Rec}) ->
    dict_to_pub_packets(UnAck1,?QOS_1) ++
    dict_to_pub_packets(UnAck2,?QOS_2) ++
    set_to_pubrel_packets(Rec).


%% MQTT-4.6 is not amazingly clear on how re-sent messages need to be ordered.
%% Just in case, we rely on the following properties to ensure that packets are re-sent sequentially:
%% - user orddict ond ordsets to store packets and their states
%% - use sequentially assigned PSeqs that map to and from PacketIds
dict_to_pub_packets(Dict,QoS) ->
    [to_publish(CTRPacket,QoS,PSeq,true) || {PSeq,CTRPacket}  <- orddict:to_list(Dict)].

set_to_pubrel_packets(Rec) ->
    [to_pubrel(PSeq) || {PSeq,PSeq}  <- ordsets:to_list(Rec)].

to_publish(#packet{topic = Topic,content = Content,ref = Retain,qos = QoS},QoS,PSeq,Dup) ->
    #'PUBLISH'{content = Content,
               packet_id = get_packet_id(PSeq),
               qos = QoS,
               topic = Topic,
               dup = Dup,
               retain = Retain}.

to_pubrel(PSeq) ->
    #'PUBREL'{packet_id = get_packet_id(PSeq)}.

%% @doc
%% Maps a 16-bit PacketId to a potentially larger Sequence number
%% @end
get_ack_seq(PacketId,PSeq) -> ((PSeq band 16#ffff) bxor PSeq) bor PacketId.
%% @doc
%% Maps a potentially large Sequence number to a PacketId
%% @end
get_packet_id(PSeq) -> PSeq band 16#ffff.

new() -> new(?DEFAULT_MAX_WINDOW).


new(MaxWnd) ->
    #outgoing{
        window_size = MaxWnd,
        packet_seq = 0,
        qos1 = orddict:new(),
        qos2 = orddict:new(),
        qos2_rec = ordsets:new(),
        refs = gb_sets:new(),
        subs = min_val_tree:new()
    }.


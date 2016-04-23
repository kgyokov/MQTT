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

-include("mqtt_internal_msgs.hrl").
-include("mqtt_session.hrl").
%% API
-export([
    push/3,
    subscribe/2,
    unsubscribe/2,
    get_subs/1,
    pub_ack/2,
    pub_rec/2,
    pub_comp/2,
    msg_in_flight/1,
    new/0,
    new/1,
    find_sub/2]).

-define(DEFAULT_MAX_WINDOW,20).


-record(outgoing,{
    subs                      ::any(),               %% Subscriptions
    queue                     ::queue:queue(),
    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    window_size               ::non_neg_integer(),   %% the max number of msgs in flight
    packet_seq                ::non_neg_integer(),   %% The latest client-assigned packet id
    %% Incremented by 1 for every packet,
    %% (though that is not required by the protocol)
    qos1,                       %% Unacknowledged QoS1 messages
    qos2,                       %% Unacknowledged QoS2 messages
    qos2_rec
}).



%% ================================================================================
%% SUBSCRIPTIONS
%% ================================================================================

find_sub(Filter,#outgoing{subs = Subs}) ->
    orddict:find(Filter,Subs).

-spec(get_subs(#outgoing{}) -> [{binary(),qos(),any()}]).

get_subs(#outgoing{subs = Subs}) ->
    [{Filter,QoS,Seq} || {Filter,{QoS,Seq}} <- orddict:to_list(Subs)].

%% @doc
%% Adds new subscriptions to the session data
%% @end
-spec subscribe(NewSubs::[{binary(),qos()}],#outgoing{}) ->
    #outgoing{}.

subscribe(NewSubs,S = #outgoing{subs = Subs}) ->
    Subs1 = lists:foldl(fun maybe_add_sub/2,Subs,NewSubs),
    S#outgoing{subs = Subs1}.

maybe_add_sub({Filter,QoS},Subs) ->
    NewSub =
        case orddict:find(Filter,Subs) of
            {ok,{_,Seq}} -> {QoS,Seq};
            error        -> {QoS,undefined}
        end,
    orddict:store(Filter,NewSub,Subs).

%% @doc
%% Removes existing subscriptions from the session data
%% Also flushes the queue, although that is not really necessary according to the spec
%% 3.10.4 - "It MAY continue to deliver any existing messages buffered for delivery to the Client"
%% @end
-spec unsubscribe([binary()],#outgoing{}) -> #outgoing{}.

unsubscribe(OldSubs,S = #outgoing{subs = Subs,queue = Q}) ->
    Subs1 = lists:foldl(fun orddict:erase/2,Subs,OldSubs),
    S#outgoing{subs = Subs1, queue = flush_queue(OldSubs,Q)}.

flush_queue(OldSubs,Q) ->
    queue:filter(fun({Filter,_}) -> lists:member(Filter,OldSubs) end,Q).


%% =========================================================================
%% MESSAGES
%% =========================================================================

push(Filter,Packet = #packet{seq = Seq},SO = #outgoing{subs = Subs}) ->
    case should_accept(Filter,Subs) of
        false -> {[],SO};
        true ->
            SO1 = SO#outgoing{subs = orddict:update(Filter,
                                            fun({QoS,_}) -> {QoS,Seq} end,
                                    Subs)},
            maybe_enqueue(Packet,SO1)
    end.

maybe_enqueue(Msg,S = #outgoing{queue = Q,window_size = WSize}) when WSize =<0 ->
    {[],S#outgoing{queue = queue:cons(Msg,Q)}};

maybe_enqueue(Msg,S = #outgoing{window_size = WSize}) ->
    move_to_session(Msg,S#outgoing{window_size = WSize - 1}).

move_to_session(Packet,SO = #outgoing{subs = Subs}) ->
    Packet1 = set_actual_qos(Subs,Packet),
    push_to_session(Packet1,SO).

pull(S = #outgoing{queue = Q,window_size = WSize}) ->
    case queue:is_empty(Q) of
        true -> {[],S#outgoing{window_size = WSize+1}};
        false ->
            S1 = S#outgoing{queue = queue:tail(Q)},
            Msg = queue:head(Q),
            move_to_session(Msg,S1)
    end.

set_actual_qos(Subs,Packet = #packet{topic = Topic,
                                     qos = MsgQoS}) ->
    SubL = [{SubFilter,SubQoS} || {SubFilter,{SubQoS,_}} <-orddict:to_list(Subs)],
    {ok,{_,SubQos}} = mqtt_topic:best_match(SubL,Topic),
    ActualQoS = min(MsgQoS,SubQos),
    Packet#packet{qos = ActualQoS}.

should_accept(Filter,Subs) ->
    case orddict:find(Filter,Subs) of
        {ok,_Sub} -> true;
        error     -> false
    end.

push_to_session(Packet = #packet{qos =?QOS_0},SO) ->
    Pub = to_publish(Packet,undefined,false),
    {ToSend,SO1} = pull(SO),
    {[Pub|ToSend],SO1};

push_to_session(Packet = #packet{qos = QoS},SO) when QoS =:= ?QOS_1;
                                                     QoS =:= ?QOS_2  ->
    #outgoing{packet_seq = PSeq} = SO,
    PSeq1 = PSeq + 1,
    SO1 = add_to_outgoing(PSeq1,Packet,SO),
    SO2 = SO1#outgoing{packet_seq = PSeq1},
    Pub = to_publish(Packet,PSeq1,false),
    {[Pub],SO2}.

add_to_outgoing(PSeq,Packet = #packet{qos = ?QOS_1},SO) ->
    #outgoing{qos1 = QosQueue} = SO,
    SO#outgoing{qos1 = orddict:store(PSeq,Packet,QosQueue)};

add_to_outgoing(PSeq,Packet = #packet{qos = ?QOS_2},SO) ->
    #outgoing{qos2 = QosQueue} = SO,
    SO#outgoing{qos2 = orddict:store(PSeq,Packet,QosQueue)}.

%% ==================================================================
%% Message Acknowledgements
%% ==================================================================

-spec pub_ack(packet_id(),#outgoing{}) ->
    {[#'PUBLISH'{}],#outgoing{}}.

pub_ack(PacketId,SO = #outgoing{packet_seq = PSeq,
                                        qos1 = QoS1Msgs}) ->
    AckSeq = get_ack_seq(PacketId,PSeq),
    case orddict:find(AckSeq,QoS1Msgs) of
        {ok,_} ->
            SO1 = SO#outgoing{qos1 = orddict:erase(AckSeq,QoS1Msgs)},
            pull(SO1);
        error ->
            {[],SO}
    end.

-spec pub_rec(packet_id(),#outgoing{}) ->
    {[#'PUBREL'{}],#outgoing{}}.

pub_rec(PacketId,SO) ->
    #outgoing{qos2 = Msgs,
              qos2_rec = Ack,
              packet_seq = PSeq} = SO,
    AckSeq = get_ack_seq(PacketId,PSeq),
    SO1 =
        case orddict:find(AckSeq,Msgs) of
            {ok,_} ->
                {ok,
                    SO#outgoing{qos2 = orddict:erase(AckSeq,Msgs),
                                qos2_rec = ordsets:add_element(AckSeq,Ack)}
                };
            error -> SO
        end,
    {[to_pubrel(PacketId)],SO1}.


-spec pub_comp(packet_id(),#outgoing{}) ->
    {[#'PUBLISH'{}],#outgoing{}}.

pub_comp(PacketId,SO = #outgoing{packet_seq = PSeq,
                                 qos2_rec = Ack}) ->
    AckSeq = get_ack_seq(PacketId,PSeq),
    case ordsets:is_element(AckSeq,Ack) of
        true ->
            SO1 = SO#outgoing{qos2_rec = ordsets:del_element(AckSeq,Ack)},
            pull(SO1);
        false ->
            {[],SO}
    end.

%% =========================================================================
%% RECOVERY
%% =========================================================================

%% [MQTT-4.4.0-1] "When a Client reconnects with CleanSession set to 0, both the Client and Server MUST
%% re-send any unacknowledged PUBLISH Packets (where QoS > 0) and PUBREL Packets using their original
%% Packet Identifiers"
msg_in_flight(#outgoing{qos1 = UnAck1,
                        qos2 = UnAck2,
                        qos2_rec = Rec}) ->
    dict_to_pub_packets(UnAck1) ++
    dict_to_pub_packets(UnAck2) ++
    set_to_pubrel_packets(Rec).


%% MQTT-4.6 is not amazingly clear on how re-sent messages need to be ordered.
%% Just in case, we rely on the following properties to ensure that packets are re-sent sequentially:
%% - user orddict ond ordsets to store packets and their states
%% - use sequentially assigned PSeqs that map to and from PacketIds
dict_to_pub_packets(Dict) ->
    [to_publish(CTRPacket,PSeq,true) || {PSeq,CTRPacket}  <- orddict:to_list(Dict)].

set_to_pubrel_packets(Rec) ->
    [to_pubrel(PSeq) || {PSeq,PSeq}  <- ordsets:to_list(Rec)].

to_publish(#packet{topic = Topic,
                   content = Content,
                   retain = Retain,
                   qos = QoS},PSeq,Dup) ->
    #'PUBLISH'{content = Content,
               packet_id = maybe_get_packet_id(PSeq),
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

maybe_get_packet_id(undefined) -> undefined;
maybe_get_packet_id(PSeq) -> get_packet_id(PSeq).

%% @doc
%% Maps a potentially large Sequence number to a PacketId
%% @end
get_packet_id(PSeq) -> PSeq band 16#ffff.

new() -> new(?DEFAULT_MAX_WINDOW).


new(MaxWnd) ->
    #outgoing{
        window_size = MaxWnd,
        subs = orddict:new(),
        queue = queue:new(),

        packet_seq = 0,
        qos1 = orddict:new(),
        qos2 = orddict:new(),
        qos2_rec = ordsets:new()
    }.





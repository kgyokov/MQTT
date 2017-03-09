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
    find_sub/2,
    set_seq/2, take_any/1]).

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

%% @todo: simplify

%% ================================================================================
%% SUBSCRIPTIONS
%% ================================================================================

find_sub(Filter,#outgoing{subs = Subs}) ->
    orddict:find(Filter,Subs).

sub_exists(Filter,Subs) ->
    case orddict:find(Filter,Subs) of
        {ok,_Sub} -> true;
        error     -> false
    end.

-spec(get_subs(#outgoing{}) -> [{binary(),qos(),any()}]).

get_subs(#outgoing{subs = Subs}) ->
    [{Filter,QoS,Seq} || {Filter,{QoS,Seq}} <- orddict:to_list(Subs)].

%% @doc
%% Adds new subscriptions to the session data
%% @end
-spec subscribe(NewSubs::[{binary(),qos()}],#outgoing{}) ->
    {[{binary(),qos(),any()}], #outgoing{}}.

subscribe(NewSubs,S = #outgoing{subs = Subs}) ->
    {Results,Subs1} = lists:mapfoldl(fun maybe_add_sub/2,Subs,NewSubs),
    {Results, S#outgoing{subs = Subs1}}.

maybe_add_sub({Filter,QoS},Subs) ->
    NewSub =
        case orddict:find(Filter,Subs) of
            {ok,{_,Seq}} -> {pending,QoS,Seq};
            error        -> {pending,QoS,undefined}
        end,
    {_,QoS1,Seq1} = NewSub,
    {{Filter,QoS1,Seq1},orddict:store(Filter,NewSub,Subs)}.

set_seq(FiltersSeq,S = #outgoing{subs = Subs}) ->
    S#outgoing{subs = lists:foldl(fun set_seq_for_filter/2,Subs,FiltersSeq)}.

set_seq_for_filter({Filter,Seq},Subs) ->
    case orddict:find(Filter,Subs) of
        {ok,{pending,QoS,CurSeq}}   -> orddict:store(Filter,{QoS,mqtt_seq:max(CurSeq,Seq)},Subs);
        {ok,{QoS,CurSeq}}           -> orddict:store(Filter,{QoS,mqtt_seq:max(CurSeq,Seq)},Subs)
    end.

%% @doc
%% Removes existing subscriptions from the session data
%% Also flushes the queue, although that is not really necessary according to the spec
%% 3.10.4 - "It MAY continue to deliver any existing messages buffered for delivery to the Client"
%% @end
-spec unsubscribe([binary()],#outgoing{}) -> #outgoing{}.

unsubscribe(OldSubs,S = #outgoing{subs = Subs,queue = Q}) ->
    Subs1 = lists:foldl(fun orddict:erase/2,Subs,OldSubs),
    S#outgoing{subs = Subs1,queue = flush_queue(OldSubs,Q)}.

flush_queue(OldSubs,Q) ->
    queue:filter(fun({Filter,_}) -> lists:member(Filter,OldSubs) end,Q).


%% =========================================================================
%% MESSAGES
%% =========================================================================

-spec push(binary(),#packet{},#outgoing{}) -> {[#'PUBLISH'{}],#outgoing{}}.

push(Filter,Packet = #packet{seq = Seq},SO = #outgoing{subs = Subs}) ->
    case sub_exists(Filter,Subs) of
        false -> {[],SO};
        true ->
            SO1 = SO#outgoing{subs = set_seq_for_filter({Filter,Seq},Subs)},
            process_or_enqueue(Packet,SO1)
    end.

process_or_enqueue(Msg = #packet{qos = QoS},S = #outgoing{window_size = WSize})
    when WSize > 0; QoS =:= ?QOS_0  ->
    maybe_save_in_session(Msg,S);

process_or_enqueue(Msg,S = #outgoing{queue = Q})->
    {[],S#outgoing{queue = queue:cons(Msg,Q)}}.

take_any(S) -> take(0,S).

take(Num,S = #outgoing{queue = Q,window_size = WSize}) ->
    CanTake = Num + WSize,
    {F,B} = queue:split(CanTake,Q),
    Taken = queue:len(F),
    {[ to_publish(El)|| El <- queue:to_list(F)],
        S#outgoing{window_size = CanTake - Taken,queue = B}}.

maybe_save_in_session(Packet = #packet{qos =?QOS_0},SO) ->
    Pub = to_publish(Packet),
    {[Pub],SO};

maybe_save_in_session(Packet = #packet{qos = QoS},SO) when QoS =:= ?QOS_1;
                                                     QoS =:= ?QOS_2  ->
    #outgoing{packet_seq = PSeq,
              window_size = WSize} = SO,

    PSeq1 = PSeq + 1,
    Pub = to_publish(Packet,PSeq1,false),

    WSize1 = WSize - 1,
    SO1 = add_to_outgoing(PSeq1,Packet,SO),
    SO2 = SO1#outgoing{packet_seq = PSeq1,
                       window_size = WSize1},
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
            take(1,SO1);
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
                SO#outgoing{qos2 = orddict:erase(AckSeq,Msgs),
                            qos2_rec = ordsets:add_element(AckSeq,Ack)};
            error -> SO
        end,
    {[to_pubrel(PacketId)],SO1}.


-spec pub_comp(packet_id(),#outgoing{}) ->
    {[#'PUBLISH'{}],#outgoing{}}.

pub_comp(PacketId,SO = #outgoing{packet_seq = PSeq,
                                 qos2_rec = Ack}) ->
    AckSeq = get_ack_seq(PacketId,PSeq),
    SO1 = SO#outgoing{qos2_rec = ordsets:del_element(AckSeq,Ack)},
    take(1,SO1).

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
%% - user orddict and ordsets to store packets and their states
%% - use sequentially assigned PSeqs that map to and from PacketIds
dict_to_pub_packets(Dict) ->
    [to_publish(Packet,PSeq,true) || {PSeq,Packet}  <- orddict:to_list(Dict)].

set_to_pubrel_packets(Rec) ->
    [to_pubrel(PSeq) || {PSeq,PSeq}  <- ordsets:to_list(Rec)].

to_publish(Packet)          -> to_publish_msg(Packet,undefined,false).
to_publish(Packet,PSeq,Dup) -> to_publish_msg(Packet,get_packet_id(PSeq),Dup).

%%to_publish(Packet,PSeq,Dup) -> to_publish(Packet,get_packet_id(PSeq),Dup).

to_publish_msg(#packet{topic = Topic,
                       content = Content,
                       retain = Retain,
                       qos = QoS},PacketId,Dup) ->
    #'PUBLISH'{content = Content,
               packet_id = PacketId,
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

%%%% @doc
%%%% Maps a potentially large Sequence number to a PacketId
%%%% @end
%%maybe_get_packet_id(undefined) -> undefined;
%%maybe_get_packet_id(PSeq) -> get_packet_id(PSeq).

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





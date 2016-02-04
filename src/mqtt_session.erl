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
-export([append_msg/3,
         append_message_comp/2,
         message_ack/2,
         message_pub_rec/2,
         message_pub_comp/2,
         msg_in_flight/1,
         subscribe/2,
         unsubscribe/2,
         new/0,
         to_publish/5,
         to_pubrel/1,
         get_subs/1,
         append_retained/3]).



%% ================================================================================
%% SUBSCRIPTIONS
%% ================================================================================

%% @doc
%% Adds new subscriptions to the session data
%% @end
subscribe(NewSubs,S = #session_out{subs = Subs}) ->
    %% Maintain a flat list of subscriptions
    %% @todo: Optimize/deduplicate
    Subs1 = lists:foldr(fun({Filter,QoS},Acc) ->
                                orddict:store(Filter,QoS,Acc)
                              end,
                             Subs, NewSubs),
    %% @todo: Deduplicate
    S#session_out{subs = Subs1}.

%% @doc
%% Removed existing subscriptions from the session data
%% @end
unsubscribe(OldSubs,S = #session_out{subs = Subs}) ->
    Subs1= lists:foldr(fun orddict:erase/2,Subs, OldSubs),
    S#session_out{subs = Subs1}.


%% =========================================================================
%% MESSAGES
%% =========================================================================

%% @doc
%% Appends message for delivery
%% @end
append_msg(CTRPacket = {_Topic,_Content,Ref},QoS,Session) ->
    #session_out{refs = Refs, subs = _Subs} = Session,
    case gb_sets:is_member(Ref,Refs) of
        false -> forward_msg(Session,CTRPacket,QoS);
        true  -> duplicate
    end.

forward_msg(CTRPacket = {_Topic,_Content,Ref},QoS,SO) ->
    #session_out{packet_seq = PacketSeq, refs = Refs} = SO,

    NewPacketSeq = PacketSeq+1,
    PacketId = if QoS =:= ?QOS_1;
                  QoS =:= ?QOS_2 ->
                        NewPacketSeq band 16#ffff;
                  true ->
                        undefined
               end,
    Session1 = SO#session_out{refs = gb_sets:add(Ref,Refs)},
    Session2 = case QoS of
                      ?QOS_1 ->
                          #session_out{qos1 = QosQueue} = Session1,
                          Session1#session_out{qos1 = orddict:store(PacketId,CTRPacket,QosQueue),
                                               packet_seq = NewPacketSeq};
                      ?QOS_2 ->
                          #session_out{qos2 = QosQueue} = Session1,
                          Session1#session_out{qos2 = orddict:store(PacketId,CTRPacket,QosQueue),
                                               packet_seq = NewPacketSeq};
                      ?QOS_0 ->
                          SO
                  end,
%%       #session_out{refs = gb_sets:add(Ref,Refs)},
    {ok,Session2,PacketId}.

append_message_comp(Ref,SO = #session_out{refs = Refs}) ->
    SO#session_out{refs = gb_sets:del_element(Ref,Refs)}.

message_ack(PacketId,SO) ->
    #session_out{qos1 = Msgs} = SO,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            {ok,
             SO#session_out{qos1 = orddict:erase(PacketId,Msgs)}};
        error ->
            duplicate
    end.


message_pub_rec(PacketId,SO) ->
    #session_out{qos2 = Msgs, qos2_rec = Ack} = SO,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            {ok,
                SO#session_out{qos2 = orddict:erase(PacketId,Msgs),
                               qos2_rec = gb_sets:add(PacketId,Ack)}};
        error ->
            duplicate
    end.


message_pub_comp(PacketId,SO) ->
    #session_out{qos2_rec = Ack} = SO,
    case gb_sets:is_member(PacketId,Ack) of
        true ->
            {ok,
                SO#session_out{qos2_rec = gb_sets:delete(PacketId,Ack)}};
        false ->
            duplicate
    end.

append_retained(NewSubs,Retained,SO) ->
    SO1 = subscribe(NewSubs,SO),
    %% Get the retained messages
    Msgs =
    [   begin
            {ok,{_,SubQos}} = mqtt_topic:best_match(NewSubs,Topic),
            {Topic,Content,Ref,min(SubQos,MsgQoS)}
        end
        ||{Topic,Content,Ref,MsgQoS} <- Retained],
    %% Apply them to session
    {Results,SO2} = append_msgs(Msgs,SO1),
    PkToSend =
        [to_publish(CTRPacket,true,QoS,PacketId,false)
            || {ok,CTRPacket,QoS,PacketId} <- Results],
    {SO2,PkToSend}.

get_subs(#session_out{subs = Subs}) ->
    orddict:to_list(Subs).

append_msgs(Msgs,SO) ->
    lists:mapfoldl(
        fun({Topic,Content,Ref,QoS},SOAcc) ->
            CTRPacket = {Topic,Content,Ref},
            case append_msg(CTRPacket,QoS,SOAcc) of
                duplicate            -> {duplicate,SOAcc};
                {ok,SOAcc1,PacketId} -> {{ok,CTRPacket,QoS,PacketId},SOAcc1}
            end
        end,
        SO,Msgs).

%% =========================================================================
%% RECOVERY
%% =========================================================================

msg_in_flight(SO) ->
    retry_in_flight(SO).

%% Retries messages persisted in session
retry_in_flight(#session_out{qos1 = UnAck1,
                             qos2 = UnAck2,
                             qos2_rec = Rec}) ->
    dict_to_pub_packets(UnAck1,?QOS_1) ++
    dict_to_pub_packets(UnAck2,?QOS_2) ++
    [to_pubrel(PacketId) || {PacketId,PacketId}  <- gb_sets:to_list(Rec)].

dict_to_pub_packets(Dict,QoS) ->
    [to_publish(CTRPacket,false,QoS,PacketId, true) || {PacketId,CTRPacket}  <- orddict:to_list(Dict)].

to_publish({Topic,Content,_Ref},Retain,QoS,PacketId,Dup) ->
    #'PUBLISH'{content = Content,packet_id = PacketId,
               qos = QoS,topic = Topic,
               dup = Dup,retain = Retain}.

to_pubrel(PacketId) ->
    #'PUBREL'{packet_id = PacketId}.


new() ->
    #session_out{
        packet_seq = 0,
        qos1 = orddict:new(),
        qos2 = orddict:new(),
        qos2_rec = gb_sets:new(),
        refs = gb_sets:new(),
        subs = orddict:new()
    }.


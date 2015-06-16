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
subscribe(S = #session_out{subs = Subs},NewSubs) ->
    %% Maintain a flat list of subscriptions
    %% @todo: Optimize/deduplicate
    Subs1 = lists:foldr(fun({Topic,QoS},Acc) ->
                                orddict:store(Topic,QoS,Acc)
                              end,
                             Subs, NewSubs),
    %% @todo: Deduplicate
    S#session_out{subs = Subs1}.

%% @doc
%% Removed existing subscriptions from the session data
%% @end
unsubscribe(S = #session_out{subs = Subs},OldSubs) ->
    S#session_out{subs =
                  lists:foldr(fun(Topic,Acc) ->
                                orddict:erase(Topic,Acc)
                              end,
                              Subs, OldSubs)}.


%% =========================================================================
%% MESSAGES
%% =========================================================================

%% @doc
%% Appends message for delivery
%% @end
append_msg(Session,CTRPacket = {_Topic,_Content,Ref},QoS) ->
    #session_out{refs = Refs, subs = _Subs} = Session,
    case gb_sets:is_member(Ref,Refs) of
        false -> forward_msg(Session,CTRPacket,QoS);
        true  -> duplicate
    end.

forward_msg(Session,CTRPacket = {_Topic,_Content,Ref},QoS) ->
    #session_out{packet_seq = PacketSeq, refs = Refs} = Session,

    NewPacketSeq = PacketSeq+1,
    PacketId = if QoS =:= ?QOS_1;
                  QoS =:= ?QOS_2 ->
                        NewPacketSeq band 16#ffff;
                  true ->
                        undefined
               end,
    Session1 = Session#session_out{refs = gb_sets:add(Ref,Refs)},
    NewSession = (case QoS of
                      ?QOS_1 ->
                          #session_out{qos1 = QosQueue} = Session1,
                          Session1#session_out{qos1 = orddict:store(PacketId,CTRPacket,QosQueue),
                                               packet_seq = NewPacketSeq};
                      ?QOS_2 ->
                          #session_out{qos2 = QosQueue} = Session1,
                          Session1#session_out{qos2 = orddict:store(PacketId,CTRPacket,QosQueue),
                                               packet_seq = NewPacketSeq};
                      ?QOS_0 ->
                          Session
                  end),
%%       #session_out{refs = gb_sets:add(Ref,Refs)},
    {ok,NewSession,PacketId}.

append_message_comp(Session = #session_out{refs = Refs}, Ref) ->
    Session#session_out{refs = gb_sets:del_element(Ref,Refs)}.

message_ack(Session,PacketId) ->
    #session_out{qos1 = Msgs} = Session,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            {ok,
             Session#session_out{qos1 = orddict:erase(PacketId,Msgs)}};
        error ->
            duplicate
    end.


message_pub_rec(Session,PacketId) ->
    #session_out{qos2 = Msgs, qos2_rec = Ack} = Session,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            {ok,
             Session#session_out{qos2 = orddict:erase(PacketId,Msgs),
                                 qos2_rec = gb_sets:add(PacketId,Ack)}};
        error ->
            duplicate
    end.


message_pub_comp(Session,PacketId)  ->
    #session_out{qos2_rec = Ack} = Session,
    case gb_sets:is_member(PacketId,Ack) of
        true ->
            {ok,
            Session#session_out{qos2_rec = gb_sets:delete(PacketId,Ack)}};
        false ->
            duplicate
    end.

append_retained(SO,NewSubs,Retained) ->
    error_logger:info_msg("append_retained called with: ~p, ~p,~n",[NewSubs,Retained]),
    SO1 = mqtt_session:subscribe(SO,NewSubs),
    %% Get the retained messages
    Msgs =
    [   begin
            {ok,{_,SubQos}} = mqtt_topic:best_match(NewSubs,Topic),
            {Topic,Content,Ref,min(SubQos,MsgQoS)}
        end
        ||{Topic,Content,Ref,MsgQoS} <- Retained],
    %% Apply them to session
    {Results,SO3} = lists:mapfoldl(
        fun(Msg,SOAcc) ->
            {Topic,Content,Ref,QoS} = Msg,
            CTRPacket = {Topic,Content,Ref},
            case mqtt_session:append_msg(SOAcc,CTRPacket,QoS) of
                duplicate ->            {duplicate,SOAcc};
                {ok,SO2,PacketId} ->    {{ok,CTRPacket,QoS,PacketId},SO2}
            end
        end,
        SO1,Msgs),
    error_logger:info_msg("Results: ~p~n",[Results]),
    PkToSend = lists:filtermap(
        fun(Result) ->
            case Result of
                {ok,CTRPacket,QoS,PacketId} ->
                    {true, mqtt_session:to_publish(CTRPacket,true,QoS,PacketId,false)};
                _ ->
                    false
            end
        end, Results),
    {SO3,PkToSend}.

get_subs(#session_out{subs = Subs}) ->
    Subs.

%% =========================================================================
%% RECOVERY
%% =========================================================================

msg_in_flight(Session) ->
    retry_in_flight(Session)
    %% recover_queued(Session)
.

%% Retries messages persisted in session
retry_in_flight(#session_out{qos1 = UnAck1,
                             qos2 = UnAck2,
                             qos2_rec = Rec}) ->
    list_to_pub_packets(UnAck1,?QOS_1) ++
    list_to_pub_packets(UnAck2,?QOS_2) ++
%%     [to_publish(CTRPacket,false,?QOS_1,PacketId, true) || {PacketId,CTRPacket}  <- orddict:to_list(UnAck1)] ++
%%     [to_publish(CTRPacket,false,?QOS_2,PacketId, true)  || {PacketId,CTRPacket}  <- orddict:to_list(UnAck2)] ++
    [to_pubrel(PacketId) || {PacketId,PacketId}  <- gb_sets:to_list(Rec)].

list_to_pub_packets(List,QoS) ->
    [to_publish(CTRPacket,false,QoS,PacketId, true) || {PacketId,CTRPacket}  <- orddict:to_list(List)].

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


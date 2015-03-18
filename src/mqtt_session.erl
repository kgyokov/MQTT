%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Feb 2015 5:37 PM
%%%-------------------------------------------------------------------
-module(mqtt_session).
-author("Kalin").

%%-include("mqtt_packets.hrl").
-include("mqtt_session.hrl").
%% API
-export([append_msg/3,
         append_message_comp/2,
         message_ack/2,
         message_pub_rec/2,
         message_pub_comp/2,
         recover/1,
         subscribe/2,
         unsubscribe/2,
         cleanup/1,
         new/1]).


subscribe(S = #session_out{subscriptions = Subs,client_id = ClientId},NewSubs) ->
%%     DistinctSubs = mqtt_topic:min_cover(NewSubs),
%%     [ {add_or_replace_sub(ClientId,NewSub,Subs),NewSub} || NewSub <- DistinctSubs ],
%%     Session#session_out{subscriptions = []}.
    %% @todo: Deduplicate subscription, optimize overlapping subs
    _MinCover = mqtt_topic:min_cover(NewSubs),
    [
        mqtt_sub_repo:add_sub(ClientId,Topic,QoS)
        || {Topic,QoS} <- NewSubs
    ],
    S#session_out{subscriptions =
                  lists:foldr(fun({Topic,QoS},Acc) ->
                                orddict:store(Topic,QoS,Acc)
                              end,
                              Subs, NewSubs)}
    %% @todo: Deduplicate, persist
.

unsubscribe(S = #session_out{subscriptions = Subs, client_id = ClientId},OldSubs) ->
    [
        mqtt_sub_repo:remove_sub(ClientId,Topic)
        || Topic <- Subs
    ],
    S#session_out{subscriptions =
                  lists:foldr(fun(Topic,Acc) ->
                                orddict:erase(Topic,Acc)
                              end,
                              Subs, OldSubs)}
    %% @todo: Deduplicate, persist
    .


%% e.g. at shutdown with ClearSession = false
cleanup(#session_out{subscriptions = Subs, client_id = ClientId, is_persistent = false}) ->
    [ mqtt_sub_repo:remove_sub(ClientId,Topic) || {Topic,_QoS}  <- Subs ];

cleanup(_S) ->
    ok.


%%add_or_replace_sub(ClientId,{Topic,QoS},Subs)->
%%     State = case orddict:find(Topic,Subs) of
%%                 error ->
%%                     new;
%%                 {ok,CurrentQoS} when CurrentQoS =/= QoS ->
%%                     replaced;
%%                 {ok,CurrentQoS} when CurrentQoS =:= QoS ->
%%                     exists
%%             end,
%%     mqtt_sub_repo:add_sub(ClientId,Topic,QoS),
%%     {State,orddict:store(Topic,QoS,Subs)}.

%% @doc
%% Appends message for delivery
%% @end

append_msg(Session,CTRPacket = {_Topic,_Content,_Retain,_QoS},Ref) ->
    #session_out{refs = Refs, subscriptions = _Subs} = Session,

    case gb_sets:is_member(Ref,Refs) of
        false ->
%%             MatchingSub = orddict:find(Topic,Subs),
%%             case MatchingSub of
%%                 error ->
%%                     mismatched_sub;
%%                 SubQoS ->
%%                     QoS = min(QoS,SubQoS),
%%                     forward_msg(Session,CTRPacket,Ref)
%%             end;
            forward_msg(Session,CTRPacket,Ref);
        true ->
            duplicate
    end.

forward_msg(Session,CTRPacket = {_Content,_Topic,_Retain,QoS},Ref)->
    #session_out{packet_seq = PacketSeq, refs = Refs} = Session,

    PacketId = if QoS =:= ?QOS_AT_MOST_ONCE;
                  QoS =:= ?QOS_AT_LEAST_ONCE ->
                        (PacketSeq+1) band 16#ffff;
                  true ->
                        undefined
               end,
    Session1 = Session#session_out{refs = gb_sets:add(Ref,Refs)},
    NewSession = (case QoS of
                      ?QOS_AT_LEAST_ONCE ->
                          #session_out{qos1 = QosQueue} = Session1,
                          Session1#session_out{qos1 = orddict:store(PacketId,CTRPacket,QosQueue),
                              packet_seq = PacketId};
                      ?QOS_EXACTLY_ONCE ->
                          #session_out{qos2 = QosQueue} = Session1,
                          Session1#session_out{qos2 = orddict:store(PacketId,CTRPacket,QosQueue),
                              packet_seq = PacketId};
                      ?QOS_AT_MOST_ONCE ->
                          Session
                  end),
%%       #session_out{refs = gb_sets:add(Ref,Refs)},
    {ok,NewSession,false}
.

append_message_comp(Session = #session_out{refs = Refs}, Ref) ->
    Session#session_out{refs = gb_sets:delete(Ref,Refs)}.

message_ack(Session,PacketId) ->
    #session_out{qos1 = Messages} = Session,
    Session#session_out{qos1 = orddict:erase(PacketId,Messages)}.


message_pub_rec(Session,PacketId) ->
    #session_out{qos2 = Msgs, qos2_rec = Ack} = Session,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            Session#session_out{qos2 = orddict:erase(PacketId,Msgs),
                qos2_rec = gb_sets:add(PacketId,Ack)};
        error ->
            Session
    end.


message_pub_comp(Session = #session_out{},PacketId)  ->
    #session_out{qos2_rec = Ack} = Session,
    Session#session_out{qos2_rec = gb_sets:delete(PacketId,Ack)}.

recover(Session) ->
    recover_in_flight(Session),
    get_retained(Session),
    recover_queued(Session)
.

recover_in_flight(#session_out{qos1 = UnAck1, qos2 = UnAck2,
    qos2_rec = Rec, packet_seq = PacketSeq}) ->
    NewPackets =
        [ to_publish(?QOS_AT_LEAST_ONCE,Packet,true) || Packet  <- orddict:to_list(UnAck1)] ++
        [ to_publish(?QOS_AT_MOST_ONCE,Packet,true)  || Packet  <- orddict:to_list(UnAck2)] ++
        [ to_pubrel(PacketId) || {PacketId,PacketId}  <- gb_sets:to_list(Rec)],
    {PacketSeq,NewPackets}.

recover_queued(_Session) ->
    ok.

get_retained(_Session) ->
    ok.

%% to_publish(QoS,Packet) ->
%%   to_publish(QoS,Packet,true).

to_publish(QoS,{PacketId,{Content,Topic,Retain}},Dup) ->
    #'PUBLISH'{content = Content,packet_id = PacketId,
        qos = QoS,topic = Topic,
        dup = Dup,retain = Retain}.

to_pubrel(PacketId) ->
    #'PUBREL'{packet_id = PacketId}.


new(ClientId)->
    #session_out{
        client_id = ClientId,
        packet_seq = 0,
        qos1 = orddict:new(),
        qos2 = orddict:new(),
        qos2_rec = gb_sets:new(),
        refs = gb_sets:new(),
        subscriptions = orddict:new()
    }.
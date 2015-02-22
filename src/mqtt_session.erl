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

-include("mqtt_packets.hrl").
-include("mqtt_session.hrl").
%% API
-export([append_message/2, message_ack/2, message_pub_rec/2, message_pub_comp/2, recover/1]).


%%
%% @doc
%% Appends message
%%
%%
%%
%% @end
append_message(Session = #mqtt_session{},{Content,Topic,Retain,{QoS,Ref}}) ->
  #mqtt_session{packet_id = PacketId,refs = Refs}=Session,
  NewPacketId = (PacketId+1) band 16#ffff,
  NewSession = (case QoS of
                  ?QOS_AT_LEAST_ONCE ->
                    #mqtt_session{qos1 = QosQueue} = Session,
                    Session#mqtt_session{qos1 = dict:store(NewPacketId,{Content,Topic,Retain},QosQueue)};
                  ?QOS_EXACTLY_ONCE ->
                    #mqtt_session{qos2 = QosQueue} = Session,
                    Session#mqtt_session{qos2 = dict:store(NewPacketId,{Content,Topic,Retain},QosQueue)}
                end)
  #mqtt_session{packet_id = NewPacketId, refs = dict:store(Ref,Ref,Refs)},
  {NewSession,PacketId}.

message_ack(Session,PacketId) ->
  #mqtt_session{qos1 = Messages} = Session,
  Session#mqtt_session{ qos1 = dict:erase(PacketId,Messages)}.


message_pub_rec(Session = #mqtt_session{},PacketId) ->
  #mqtt_session{qos2 = Messages, qos2_rec = Ack} = Session,
  case dict:find(PacketId,Messages) of
    {ok,_} ->
      Session#mqtt_session{qos2 = dict:erase(PacketId,Messages),
        qos2_rec = dict:store(PacketId,PacketId,Ack)};
    error ->
      Session
  end.


message_pub_comp(Session = #mqtt_session{},PacketId)  ->
  #mqtt_session{qos2_rec = Ack} = Session,
  Session#mqtt_session{qos2 = dict:erase(PacketId,Ack)}.

recover(#mqtt_session{qos1 = UnAck1, qos2 = UnAck2,
  qos2_rec = Rec, packet_id = PacketSeq}) ->
  NewPackets =
    [ to_publish(?QOS_AT_LEAST_ONCE,Packet) || Packet  <- dict:to_list(UnAck1)] ++
    [ to_publish(?QOS_AT_MOST_ONCE,Packet)  || Packet  <- dict:to_list(UnAck2)] ++
    [ to_pubrel(PacketId) || {PacketId,PacketId}  <- dict:to_list(Rec)],
  {PacketSeq,NewPackets}.


to_publish(QoS,{PacketId,{Content,Topic,Retain}})->
  #'PUBLISH'{content = Content,packet_id = PacketId,
    qos = QoS,topic = Topic,
    dup = true,retain = Retain}.

to_pubrel(PacketId)->
  #'PUBREL'{packet_id = PacketId}.


new(ClientId)->
  #mqtt_session{
    client_id = ClientId,
    packet_id = 0,
    packet_seq = 0,
    qos1 = dict:new(),
    qos2 = dict:new(),
    qos2_rec = dict:new(),
    refs = dict:new()
  }.
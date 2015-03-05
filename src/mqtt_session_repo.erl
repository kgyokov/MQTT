%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2015 12:33 AM
%%%-------------------------------------------------------------------
-module(mqtt_session_repo).
-author("Kalin").
-include("mqtt_packets.hrl").
-include("mqtt_session.hrl").
%%-include("mqtt_pubsub.hrl").

%% API
-export([create_tables/2, append_message/2,
  clear/1, recover/1, message_ack/2, message_pub_rec/2, message_pub_comp/2]).


%% {Message,Topic,Qos,State}
%% @doc
%%
%%
%%
%%
%% @end

-define(SESSION_TABLE, mqtt_session).

-define(TABLE_DEF,[
  {type,set},
  {attributes,record_info(fields,session_out)}
]).

%%
%% @doc
%%
%% Creates the mnesia tables. To be called only once.
%%
%% @end
create_tables(Nodes,NFragments)->
  mnesia:create_schema(Nodes),
  mnesia:create_table(?SESSION_TABLE, [
    {disc_copies, Nodes},
    {frag_properties,
      {n_fragments,NFragments},
      {node_pool,Nodes}
    }
    |?TABLE_DEF]
  ).

%%
%% @doc
%% Sets up a message for delivery
%%
%%
%%
%% @end

append_message(ClientId, Package = {Content,Topic,Retain,{QoS,Ref}})->
  Fun =
    fun() ->
      Session =
        case mnesia:read(?SESSION_TABLE,ClientId, write) of
          [] ->
            new(ClientId);
          [S]->
            S
        end,
      {UpdatedSession,PacketId} = append_message(Session,Package),
      mnesia:write(UpdatedSession),
      PacketId
    end,
  PacketId = mnesia:activity(transaction,Fun),
  mqtt_connection:publish_packet(undefined,{Content,Topic,QoS,PacketId});


%%
%% @doc
%% Appends message
%%
%%
%%
%% @end
append_message(Session = #session_out{},{Content,Topic,Retain,{QoS,Ref}}) ->
  #session_out{packet_seq = PacketId,refs = Refs}=Session,
  NewPacketId = (PacketId+1) band 16#ffff,
  NewSession = (case QoS of
                  ?QOS_AT_LEAST_ONCE ->
                    #session_out{qos1 = QosQueue} = Session,
                    Session#session_out{qos1 = dict:store(NewPacketId,{Content,Topic,Retain},QosQueue)};
                  ?QOS_EXACTLY_ONCE ->
                    #session_out{qos2 = QosQueue} = Session,
                    Session#session_out{qos2 = dict:store(NewPacketId,{Content,Topic,Retain},QosQueue)}
                end)
  #session_out{packet_seq = NewPacketId, refs = dict:store(Ref,Ref,Refs)},
  {NewSession,PacketId}.

message_ack(ClientId,PacketId)  ->
  message_ack(undefined,PacketId);

message_ack(Session = #session_out{},PacketId) ->
  #session_out{qos1 = Messages} = Session,
  Session#session_out{ qos1 = dict:erase(PacketId,Messages)}.

message_pub_rec(ClientId,PacketId)->
  message_pub_rec(undefined,PacketId);

message_pub_rec(Session = #session_out{},PacketId) ->
  #session_out{qos2 = Messages, qos2_rec = Ack} = Session,
  case dict:find(PacketId,Messages) of
    {ok,_} ->
      Session#session_out{qos2 = dict:erase(PacketId,Messages),
                           qos2_rec = dict:store(PacketId,PacketId,Ack)};
    error ->
      Session
  end.

message_pub_comp(ClientId,PacketId) ->
  message_pub_comp(undefined,PacketId);

message_pub_comp(Session = #session_out{},PacketId)  ->
  #session_out{qos2_rec = Ack} = Session,
  Session#session_out{qos2 = dict:erase(PacketId,Ack)}.

clear(ClientId)->
  Fun = fun() ->
    case mnesia:read(?SESSION_TABLE,ClientId, write) of
      [] ->
        ok;
      [_] ->
        mnesia:write(new(ClientId))
    end
  end,
  mnesia:activity(transaction,Fun).

recover(ClientId)->
  Fun = fun() ->
    case mnesia:read(?SESSION_TABLE,ClientId, read) of
      [] ->
        {0,[]};
      [Session] ->
        recover(Session)
    end
  end;



recover(#session_out{qos1 = UnAck1, qos2 = UnAck2,
                      qos2_rec = Rec, packet_seq = PacketSeq}) ->
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
  #session_out{
    client_id = ClientId,
    packet_seq = 0,
    qos1 = dict:new(),
    qos2 = dict:new(),
    qos2_rec = dict:new(),
    refs = dict:new()
  }.

client_do(ClientId,Action)  ->
ok
.


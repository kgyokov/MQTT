%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2015 12:33 AM
%%%-------------------------------------------------------------------
-module(mqtt_session).
-author("Kalin").
-include("mqtt_packets.hrl").
%%-include("mqtt_pubsub.hrl").

%% API
-export([append_message/2,
  %%append_subscription/2,
  %% remove_subscription/2,
  clear/1]).


%% {Message,Topic,Qos,State}
-record(mqtt_session,{
  client_id,
  packet_id,
  qos1,
  qos2,
  qos2_ack,
  refs
%%   qos1,
%%   qos2,
%%   qos2_acked
}).


-define(SESSION_TABLE, mqtt_session).

create_tables()->
0.


append_message(ClientId, Package = {Content,Topic,{QoS,Ref}})->
  Fun =
    fun() ->
      Session =
        case mnesia:read(?SESSION_TABLE,ClientId, write) of
          [] ->
            new(ClientId);
          [S]->
            S
        end,
      {UpdatedSession,PacketId} = append(Session,Package),

%%       [Session = #mqtt_session{outgoing_messages = CurrentMessages}]->
%%         NewMessages = [{Message,Topic,QoS,new} | CurrentMessages],
%%         mnesia:write(Session#mqtt_session{outgoing_messages = NewMessages})
      mnesia:write(UpdatedSession),
      PacketId
    end,
  PacketId = mnesia:activity(transaction,Fun),
  mqtt_connection:publish_packet(undefined,{Content,Topic,QoS,PacketId})
.

append_message(Session,{Content,Topic,{QoS,Ref}})->
%%     #mqtt_session{packet_id = PacketId, messages = Messages}=Session,
%%     Session#mqtt_session{messages = dict:append(PacketId,{Content,Topic,QoS,PacketId},Messages),
%%                          packet_id = PacketId+1 band 16#ffff}.
  #mqtt_session{packet_id = PacketId,refs=Refs}=Session,
  NewPacketId = (PacketId+1) band 16#ffff,
  NewSession = (case QoS of
%%     ?QOS_AT_MOST_ONCE ->
%%       #mqtt_session{qos0 = QosQueue} = Session,
%%       Session#mqtt_session{qos0 = appenq_to_queue(Content,Topic,QosQueue)};
     ?QOS_AT_LEAST_ONCE ->
      #mqtt_session{qos1 = QosQueue} = Session,
      Session#mqtt_session{qos1 = dict:store(NewPacketId,{Content,Topic},QosQueue)};
    ?QOS_EXACTLY_ONCE ->
      #mqtt_session{qos2 = QosQueue} = Session,
      Session#mqtt_session{qos2 = dict:store(NewPacketId,{Content,Topic},QosQueue)}
  end)
  #mqtt_session{packet_id=NewPacketId, refs = dict:store(Ref,Ref,Refs)},
  {NewSession,PacketId}.

new(ClientId)->
  #mqtt_session{
    client_id = ClientId,
    packet_id = 0,
    qos1 = dict:new(),
    qos2 = dict:new(),
    qos2_ack = dict:new(),
    refs = dict:new()
    %outgoing_messages = [{new, Message,Topic,QoS}],
  }.

%% appenq_to_queue(Content,Topic,{Queue,Count})->
%%   {dict:in({Content,Topic},Queue),Count+1}.

message_ack(ClientId,PacketId)->
  message_ack(undefined,PacketId);

message_ack(Session,PacketId)->
  #mqtt_session{qos1 = Messages} = Session,
  Session#mqtt_session{ qos1 = dict:erase(PacketId,Messages)}.

message_pub_rec(ClientId,PacketId)->
  message_ack(undefined,PacketId);

message_pub_rec(Session,PacketId)->
  #mqtt_session{qos2 = Messages, qos2_ack = Ack} = Session,
  case dict:find(PacketId,Messages) of
    {ok,_}->
      Session#mqtt_session{qos2 = dict:erase(PacketId,Messages),
                           qos2_ack = dict:store(PacketId,PacketId,Ack)};
    error ->
      Session
  end.

message_pub_comp(ClientId,PacketId)->
0;

message_pub_comp(Session,PacketId)->
  #mqtt_session{qos2_ack = Ack} = Session,
  Session#mqtt_session{qos2 = dict:erase(PacketId,Ack)}.

%% append_subscription(ClientId, NewSub = {_TopicFilter,_QoS})->
%%   Fun = fun() ->
%%     case mnesia:read(?SESSION_TABLE,ClientId, write) of
%%       [] ->
%%         mnesia:write(#mqtt_session{
%%           client_id = ClientId,
%%           %outgoing_messages = [],
%%           subscriptions = [NewSub]});
%%       [Session = #mqtt_session{subscriptions = CurrentSubs}]->
%%         NewSubs = [NewSub | CurrentSubs],
%%         mnesia:write(Session#mqtt_session{subscriptions = NewSubs})
%%     end
%%   end
%% .
%%
%% remove_subscription(ClientId, TopicFilter)->
%%   Fun = fun() ->
%%     case mnesia:read(?SESSION_TABLE,ClientId, write) of
%%       [] ->
%%        ok;
%%       [Session = #mqtt_session{subscriptions = CurrentSubs}]->
%%         NewSubs = lists:filter(fun({ThisFilter,_Qos})-> ThisFilter =/= TopicFilter end, CurrentSubs),
%%         mnesia:write(Session#mqtt_session{subscriptions = NewSubs})
%%     end
%%   end
%% .

clear(ClientId)->
  Fun = fun() ->
   mnesia:delete({?SESSION_TABLE,ClientId})
  end
.

recover()->
 .



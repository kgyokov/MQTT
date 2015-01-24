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
-include("mqtt_pubsub.hrl").

%% API
-export([append_message/2, append_subscription/2, remove_subscription/2, clear/1]).


%% {Message,Topic,Qos,State}



-define(SESSION_TABLE, mqtt_session).

create_tables()->
0.

append_message(ClientId, {Message,Topic,QoS})->
  Fun = fun() ->
    case mnesia:read(?SESSION_TABLE,ClientId, write) of
      [] ->
        mnesia:write(#mqtt_session{
          client_id = ClientId,
          outgoing_messages = [{new, Message,Topic,QoS}],
          subscriptions = []});
      [Session = #mqtt_session{outgoing_messages = CurrentMessages}]->
        NewMessages = [{Message,Topic,QoS,new} | CurrentMessages],
        mnesia:write(Session#mqtt_session{outgoing_messages = NewMessages})
    end
  end
.

append_subscription(ClientId, NewSub = {_TopicFilter,_QoS})->
  Fun = fun() ->
    case mnesia:read(?SESSION_TABLE,ClientId, write) of
      [] ->
        mnesia:write(#mqtt_session{
          client_id = ClientId,
          outgoing_messages = [],
          subscriptions = [NewSub]});
      [Session = #mqtt_session{subscriptions = CurrentSubs}]->
        NewSubs = [NewSub | CurrentSubs],
        mnesia:write(Session#mqtt_session{subscriptions = NewSubs})
    end
  end
.

remove_subscription(ClientId, TopicFilter)->
  Fun = fun() ->
    case mnesia:read(?SESSION_TABLE,ClientId, write) of
      [] ->
       ok;
      [Session = #mqtt_session{subscriptions = CurrentSubs}]->
        NewSubs = lists:filter(fun({ThisFilter,_Qos})-> ThisFilter =/= TopicFilter end, CurrentSubs),
        mnesia:write(Session#mqtt_session{subscriptions = NewSubs})
    end
  end
.

clear(ClientId)->
  Fun = fun() ->
   mnesia:delete({?SESSION_TABLE,ClientId})
  end
.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2015 12:30 AM
%%%-------------------------------------------------------------------
-author("Kalin").

-record(mqtt_session, {
  client_id,
  subscriptions = [],
  outgoing_messages = [],
  incoming_messages = [] %% messages with QoS = 2 in the 2-step process of being published by the client
}).

-record(mqtt_message, {
  packet_id,
  state = new,
  content,
  topic,
  qos,
  dup = 0,
  retain = 0
}).



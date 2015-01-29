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
  outgoing_qos0 = [],
  outgoing_qos1 = [],
  outgoing_qos2 = [],

  incoming_qos0 = [], %% messages with QoS = 2 in the 2-step process of being published
  incoming_qos1 = [],
  incoming_qos2 = []
}).



%%
%% @doc
%%
%% Notes on Q0S 2 state:
%%
%%   Client                           Server
%%   (M,new)
%%
%%   Client                           Server
%%   (M,published)    PUBLISH M -->
%%
%%   Client                           Server
%%   (M,published)                    (M,publish)
%%
%%   Client                           Server
%%   (M,published)    <-- PUBREC M    (M,pubrec)
%%
%%   Client                           Server
%%   (M,pubrec)                       (M,pubrec)
%%
%%   Client                           Server
%%   (M,pubrel)     PUBREL M -->      (M,pubrec)
%%
%%   Client                           Server
%%   (M,pubrel)                      (M,pubrel)
%%
%%   Client                           Server
%%   (M,pubrel)     <-- PUBCOMP M
%%
%%   Client                           Server
%%
%%
%% @end
%%
-record(mqtt_message, {
  packet_id,
  state = new,  %% published, received, relegated
  content,
  topic,
  qos,
  dup = 0,
  retain = 0
}).



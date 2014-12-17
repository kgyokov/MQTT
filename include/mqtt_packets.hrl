%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Dec 2014 1:52 AM
%%%-------------------------------------------------------------------
-author("Kalin").

%%%%%%%%%%%%%%%%%%%%%%%
%% Connections
%%%%%%%%%%%%%%%%%%%%%%%
-record('CONNECT', {protocol_name,
  protocol_version,
  client_id,
  will_topic,
  will_message,
  will_qos,
  will_retain,
  username,
  password,
  clean_session,
  keep_alive
}).

-record(connack_flags, {session_present}).
-record('CONNACK', {flags = #connack_flags{},return_code}).
-define(CONECTION_ACCEPTED, 0).
-define(UNACCEPTABLE_PROTOCOL, 1).
-define(IDENTIFIER_REJECTED, 2).
-define(SERVER_UNAVAILABLE, 3).
-define(BAD_USERNAME_OR_PASSWORD, 4).
-define(UNAUTHORIZED, 5).
-record('DISCONNECT', {flags = #connack_flags{},return_code}).


%%%%%%%%%%%%%%%%%%%%%%%
%% Publication
%%%%%%%%%%%%%%%%%%%%%%%
-record('PUBLISH', {dup,qos,retain,topic,packet_id,content}).

-record('PUBACK', {packet_id}).

-record('PUBREC', {packet_id}).

-record('PUBREL', {packet_id}).

-record('PUBCOMP', {packet_id}).


%%%%%%%%%%%%%%%%%%%%%%%
%% Subscriptions
%%%%%%%%%%%%%%%%%%%%%%%
-record('SUBSCRIBE', {packet_id,subscriptions}).
-record(subscription, {topic_filter,qos}).

-record('SUBACK', {packet_id,return_codes=[]}).

-record('UNSUBSCRIBE', {packet_id,topic_filters}).
-record('UNSUBACK', {packet_id}).


%%%%%%%%%%%%%%%%%%%%%%%
%% PING
%%%%%%%%%%%%%%%%%%%%%%%
-record('PINGREQ', {}).
-record('PINGRESP', {}).

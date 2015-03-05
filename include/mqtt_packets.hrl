%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Dec 2014 1:52 AM
%%%-------------------------------------------------------------------
-author("Kalin").

-type packet_id() :: 0..16#ffff.
-type client_id() :: binary().

%%%%%%%%%%%%%%%%%%%%%%%
%% Connections
%%%%%%%%%%%%%%%%%%%%%%%

-record(will_details,{
  topic           ::binary(),
  message         ::binary(),
  qos             ::byte(),
  retain          ::boolean()
}).

-record('CONNECT', {
  protocol_name       ::binary(),
  protocol_version    ::byte(),
  client_id           ::client_id(),
  will                ::#will_details{},
  username            ::binary(),
  password            ::binary(),
  clean_session       ::boolean(),
  keep_alive          ::0..16#ffff
}).

%%-record(connack_flags, {session_present}).
-record('CONNACK', {return_code, session_present}).
%% Return codes
-define(CONECTION_ACCEPTED, 0).
-define(UNACCEPTABLE_PROTOCOL, 1).
-define(IDENTIFIER_REJECTED, 2).
-define(SERVER_UNAVAILABLE, 3).
-define(BAD_USERNAME_OR_PASSWORD, 4).
-define(UNAUTHORIZED, 5).
-record('DISCONNECT', {}).


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
-define(SUBSCRIPTION_FAILURE,16#80).

-record('UNSUBSCRIBE', {packet_id,topic_filters}).
-record('UNSUBACK', {packet_id}).


%%%%%%%%%%%%%%%%%%%%%%%
%% PING
%%%%%%%%%%%%%%%%%%%%%%%
-record('PINGREQ', {}).
-record('PINGRESP', {}).

%%%%%%%%%%%%%%%%%%%%%%%
%% QoS
%%%%%%%%%%%%%%%%%%%%%%%
-define(QOS_AT_MOST_ONCE, 0).
-define(QOS_AT_LEAST_ONCE, 1).
-define(QOS_EXACTLY_ONCE, 3).
-define(QOS_RESERVED, 4).
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
-type qos()       ::0..2.
-type topic()     ::binary().


%%%%%%%%%%%%%%%%%%%%%%%
%% Special Values
%%%%%%%%%%%%%%%%%%%%%%%
-define(SUBSCRIPTION_FAILURE,16#80).
-define(QOS_AT_MOST_ONCE, 0).
-define(QOS_AT_LEAST_ONCE, 1).
-define(QOS_EXACTLY_ONCE, 3).
-define(QOS_RESERVED, 4).


%%%%%%%%%%%%%%%%%%%%%%%
%% Connections
%%%%%%%%%%%%%%%%%%%%%%%

-record(will_details,{
  topic           ::topic(),
  message         ::binary(),
  qos             ::qos(),
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
-define(CONNACK_ACCEPTED, 0).
-define(CONNACK_UNACCEPTABLE_PROTOCOL, 1).
-define(CONNACK_IDENTIFIER_REJECTED, 2).
-define(CONNACK_SERVER_UNAVAILABLE, 3).
-define(CONNACK_BAD_USERNAME_OR_PASSWORD, 4).
-define(CONNACK_UNAUTHORIZED, 5).
-record('DISCONNECT', {}).


%%%%%%%%%%%%%%%%%%%%%%%
%% Publication
%%%%%%%%%%%%%%%%%%%%%%%
-record('PUBLISH', {
    dup         ::boolean(),
    qos         ::qos(),
    retain      ::boolean(),
    topic       ::topic(),
    packet_id   ::packet_id(),
    content     ::binary()
}).

-record('PUBACK', {packet_id::packet_id()}).

-record('PUBREC', {packet_id::packet_id()}).

-record('PUBREL', {packet_id::packet_id()}).

-record('PUBCOMP', {packet_id::packet_id()}).


%%%%%%%%%%%%%%%%%%%%%%%
%% Subscriptions
%%%%%%%%%%%%%%%%%%%%%%%
-record('SUBSCRIBE', {
    packet_id       ::packet_id(),
    subscriptions   ::[{TopicFilter::binary(),QoS::qos()}]
}).
%%-record(subscription, {topic_filter,qos}).

-record('SUBACK', {
    packet_id           ::packet_id(),
    return_codes=[]     ::[qos | ?SUBSCRIPTION_FAILURE]
}).

-record('UNSUBSCRIBE', {
    packet_id           ::packet_id(),
    topic_filters       ::[binary()]
}).
-record('UNSUBACK', {packet_id::packet_id()}).


%%%%%%%%%%%%%%%%%%%%%%%
%% PING
%%%%%%%%%%%%%%%%%%%%%%%
-record('PINGREQ', {}).
-record('PINGRESP', {}).


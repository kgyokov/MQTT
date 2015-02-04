%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2014 4:04 PM
%%%-------------------------------------------------------------------
-author("Kalin").

-record(parse_state, {
  readfun,
  buffer,
  max_buffer_size
}).


% Packets
-define(Reserved, 0).
-define(CONNECT, 1).
-define(CONNACK, 2).
-define(PUBLISH, 3).
-define(PUBACK, 4).
-define(PUBREC, 5).
-define(PUBREL, 6).
-define(PUBCOMP, 7).
-define(SUBSCRIBE, 8).
-define(SUBACK, 9).
-define(UNSUBSCRIBE, 10).
-define(UNSUBACK, 11).
-define(PINGREQ, 12).
-define(PINGRESP, 13).
-define(DISCONNECT, 14).
%%-define(Reserved, 15).


%% QOS
-define(QOS_AT_MOST_ONCE, 0).
-define(QOS_AT_LEAST_ONCE, 1).
-define(QOS_EXACTLY_ONCE, 3).
-define(QOS_RESERVED, 4).


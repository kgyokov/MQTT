%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Dec 2014 9:34 PM
%%%-------------------------------------------------------------------
-module(mqtt_parser_backup).
-author("Kalin").

%% API
-export([]).


%% [MQTT-2.2.1]

-record(state, { readfun = undefined, buffer }).



parse_fixed_header(<<HeaderStart:8,Rest>>)->
  Type = parse_header_start(HeaderStart),
  RemainingLength = variable_length(Rest),
  PacketId = case Type of
    { 'SUBSCRIBE', _ } -> 0;
    { 'UNSUBSCRIBE', _ } -> 0;
    { 'PUBLISH ', { _, QoS, _} } when QoS > 0 -> 0
  end
.



parse_header_start(<<PacketType:4,Flags:4>>) ->
  TypeName = case PacketType of
    0 -> 'Reserved';
    1 -> 'CONNECT';
    2 -> 'CONNACK';
    3 -> 'PUBLISH';
    4 -> 'PUBACK';
    5 -> 'PUBREC';
    6 -> 'PUBREL';
    7 -> 'PUBCOMP';
    8 -> 'SUBSCRIBE';
    9 -> 'SUBACK';
    10 -> 'UNSUBSCRIBE';
    11 -> 'UNSUBACK';
    12 -> 'PINGREQ';
    13 -> 'PINGRESP';
    14 -> 'DISCONNECT';
    15 -> 'Reserved'
  end,
  { TypeName, parse_flags(TypeName, Flags) }.

%% [MQTT-2.2.2]
parse_flags(PacketType, Flags)->
  case {PacketType, Flags } of
    {'PUBLISH',     <<Dup:1,QoS:2,Retain:1>>}-> { Dup, QoS, Retain};
    {'PUBREL',      <<2#0010>>}-> {ok};
    {'SUBSCRIBE',   <<2#0010>>} -> {ok};
    {'UNSUBSCRIBE', <<2#0010>>}-> {ok};
    {_, _}-> throw(invalid_flags) %% [MQTT-2.2.2-2]
end
.


%% parse_flags('PUBLISH',<<Dup:1,QoS:2,Retain:1>>)-> { Dup, QoS, Retain};
%% parse_flags('PUBREL',<<2#0010>>)-> {ok};
%% parse_flags('SUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags('UNSUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags(_, _)-> throw(invalid_flags). %% [MQTT-2.2.2-2]


variable_length(Bytes) -> variable_length(Bytes, 0, 1).

variable_length(<<HasMore:1,Length:7, Rest>> , Sum, Multiplier) ->
  NewSum = Sum + Length * Multiplier,
  if HasMore =:= 1 ->
    variable_length(Rest, NewSum, Multiplier * 128);
    true -> NewSum
  end;

variable_length(<<_:0>>, _, _) ->
  throw(invalid_remaining_length).



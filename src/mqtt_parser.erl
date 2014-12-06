%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Dec 2014 9:34 PM
%%%-------------------------------------------------------------------
-module(mqtt_parser).
-author("Kalin").

%% API
-export([]).


-record(state, { parse_state } ).

-record(parse_state, {
  readfun,
  buffer,
  max_buffer_size
}).

%% Communication Adaptors

read(#parse_state{ buffer = <<Buffer:Length>>, max_buffer_size = MaxBufferSize}) when Length >= MaxBufferSize ->
  throw({error, buffer_overflow });

read(S#parse_state{readfun = ReadFun, buffer = Buffer})->
  case ReadFun() of
    {ok,NewBytes} -> S#parse_state{buffer = <<Buffer,NewBytes>>}; %%append the newly retrieved bytes
    {error,Reason} -> throw({error,Reason})
  end.

%[MQTT-1.5.3]
parse_string(#parse_state{buffer = <<StrLen:16,Str:StrLen/utf8,Rest/binary>>}) ->
  {{ok,Str},Rest};
parse_string(#parse_state{buffer = <<0:16,Rest/binary>>}) ->
  {{empty},Rest};
parse_string(S)->
  parse_string(read(S)).


parse_variable_length(ReadFun, Bytes) ->
  parse_variable_length(ReadFun, Bytes, 0, 1).

parse_variable_length(ReadFun, <<HasMore:1,Length:7, Rest/binary>>, Sum, Multiplier) ->
  NewSum = Sum + Length * Multiplier,
  if HasMore =:= 1 ->
    parse_variable_length(ReadFun, Rest, NewSum, Multiplier * 128);
    _ -> {NewSum, Rest}
  end;
parse_variable_length(ReadFun, Bytes, Sum, Multiplier) ->
  parse_variable_length(ReadFun, ReadFun(Bytes), Sum, Multiplier).



%% Parsing!!!!


parse_header_start( _ReadFun, <<HeaderStart:8/binary,Rest/binary>>)->
  Result = parse_type_and_flags(HeaderStart)
  %%{ Rest, Sum } = parse_variable_length(fun() -> await_more_bytes(S) end, Rest),

;
parse_header_start(ReadFun, Bytes)->
  parse_header_start(ReadFun, ReadFun(Bytes))
.






parse_fixed_header(<<HeaderStart:8,Rest/binary>>)->
  Type = parse_header_start(HeaderStart),
  RemainingLength = variable_length(Rest),
  PacketId = case Type of
    { 'SUBSCRIBE', _ } -> 0;
    { 'UNSUBSCRIBE', _ } -> 0;
    { 'PUBLISH ', { _, QoS, _} } when QoS > 0 -> 0
  end
;
parse_fixed_header(_)->
  incomplete
.

parse_type_and_flags(<<PacketType:4,Flags:4>>) ->
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

-record(connect_flags, {}).

parse_specific_type('CONNECT',
    <<2#0:8,
    2#100:8,
    "MQTT":32,
    ProtocolLevel:8,
    UsernameFlag:1,PasswordFlag:1,WillRetain:1,WillQoS:2,WillFlag:1,CleanSession:1,_:1,
    KeppAlive:16,
    ClientIdLength:16,
    ClientId
    >>, ReadFun) ->
  { 'CONNECT',ProtocolLevel, {UsernameFlag,PasswordFlag,WillRetain,WillQoS,WillFlag,CleanSession,KeppAlive}}
.
parse_specific_type('CONNECT', Buffer = <<_:Length>>, ReadFun) when Length < 32 ->
  parse_specific_type('CONNECT', ReadFun(Buffer), ReadFun)
.
parse_specific_type('CONNECT', _, _) ->
  error
.


%% parse_flags('PUBLISH',<<Dup:1,QoS:2,Retain:1>>)-> { Dup, QoS, Retain};
%% parse_flags('PUBREL',<<2#0010>>)-> {ok};
%% parse_flags('SUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags('UNSUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags(_, _)-> throw(invalid_flags). %% [MQTT-2.2.2-2]





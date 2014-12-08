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

-include("mqtt_const.hrl").

%% API
-export([read/1, parse_string/1, read/3]).


-record(state, { parse_state } ).

%%
%%
%% Communication Adaptors
%%
%%

read(_ReadFun, MaxBufferSize, Buffer) when byte_size(Buffer) > MaxBufferSize  ->
  throw({error, buffer_overflow });
read(ReadFun, _MaxBufferSize, Buffer) ->
  case ReadFun() of
    {ok,NewFragment} -> <<Buffer/binary,NewFragment/binary>>; %% append the newly retrieved bytes
    {error,Reason} -> throw({error,Reason})
  end.

read(S = #parse_state{max_buffer_size = MaxBufferSize, buffer = Buffer, readfun = ReadFun}) ->
  S#parse_state{ buffer =  read(ReadFun, MaxBufferSize, Buffer)}
.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%      PRIMITIVES:
%%
%%      strings
%%      variable lengths
%%      etc
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%[MQTT-1.5.3]
parse_string(#parse_state{buffer = <<StrLen:16,Str:StrLen/bytes,Rest/binary>>}) ->
  {{ok,Str},Rest};
parse_string(#parse_state{buffer = <<0:16,Rest/binary>>}) ->
  {{ok, ""},Rest};
parse_string(S)->
  parse_string(read(S)).

%%
%% Parses integer using variable length-encoding
%%
parse_variable_length(ReadFun, Buffer) ->
  parse_variable_length(ReadFun, Buffer, 0, 1).

parse_variable_length(ReadFun, <<HasMore:1,Length:7, Rest/binary>>, Sum, Multiplier) ->
  NewSum = Sum + Length * Multiplier,
  if HasMore =:= 1 ->
      parse_variable_length(ReadFun, Rest, NewSum, Multiplier * 128);
    true -> {NewSum, Rest}
  end;
parse_variable_length(ReadFun, Bytes, Sum, Multiplier) ->
  parse_variable_length(ReadFun, ReadFun(Bytes), Sum, Multiplier).



%% Parsing!!!!

parse_header_start(_ReadFun, <<HeaderStart:8/binary,Rest/binary>>) ->
  Result = parse_type_and_flags(HeaderStart)
  %%{ Rest, Sum } = parse_variable_length(fun() -> await_more_bytes(S) end, Rest),

;
parse_header_start(ReadFun, Bytes)->
  parse_header_start(ReadFun, ReadFun(Bytes))
.


parse_fixed_header(<<HeaderStart:8,Rest/binary>>)->
  Type = parse_type_and_flags(HeaderStart),
  RemainingLength = parse_variable_length(Rest,0),
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

%% MQTT 3.1.2.1 - "The Protocol Name is a UTF-8 encoded string that represents the protocol name “MQTT”, capitalized as shown.
%% The string, its offset and length will not be changed by future versions of the MQTT specification."
parse_specific_type('CONNECT',
  S= #parse_state{
    buffer =
      <<ProtocolNameLength:16, %% should be 4 / "MQTT", but according to 3.1.2.1 we may want to give the server
      ProtocolName/utf8,       %% the option to proceed anyway
      ProtocolLevel:8,
      UsernameFlag:1,PasswordFlag:1,WillRetain:1,WillQoS:2,WillFlag:1,CleanSession:1,_:1,
      KeppAlive:16,

      %% Payload
      %%ClientIdLength:16,
      %%ClientId:ClientIdLength/bytes,
      %%WillTopicField:WillFlag/16,

      Payload/binary>>
  }) ->

  { 'CONNECT',
      ProtocolName,
      ProtocolLevel,
      {UsernameFlag,PasswordFlag,WillRetain,WillQoS,WillFlag,CleanSession,KeppAlive}
  },


  {{ok,ClientId}, Rest} = parse_string(S#{buffer=Payload}), %% 3.1.3.1 does not place strict limitations on the ClientId

  if WillFlag =:= 1 -> 0;

%%   S1 = add_optional_str_field({#{}, S}, will_topic, WillFlag ),
%%   %%S2 = add_optional_str_field({#{}, S1}, will_message, WillFlag ),
%%   S3 = add_optional_str_field({#{}, S2}, will_topic, WillFlag ),
%%   S4 = add_optional_str_field({#{}, S3}, will_topic, WillFlag ),

  {{ok,WillTopic}, Rest1} =  parse_string(S#{buffer=Rest}),
  {{ok,WillMessage}, Rest2} =  parse_string(S#{buffer=Rest1}),
  {{ok,UserName}, Rest3} =  parse_string(S#{buffer=Rest2}),
  {{ok,Password}, Rest4} =  parse_string(S#{buffer=Rest3})
;

parse_specific_type('CONNECT', S) ->
  parse_specific_type('CONNECT', read(S))
.

%% parse_specific_type('CONNECT', S) when byte_size(S#parse_state.buffer) < 10 ->
%%   parse_specific_type('CONNECT', read(S))
%% ;
%% parse_specific_type('CONNECT', _) ->
%%   throw(malformed_packet)
%% .

add_optional_str_field({OptionaFields#{}, S}, Key, Flag ) ->
  if Flag =:= 1 ->
    {{ok,Value},Rest} = parse_string(S),
    { OptionaFields#{ Key => Value}, S#parse_state{buffer = Rest}};
    Flag =:= 0 -> {OptionaFields, S}
  end
.

%% parse_flags('PUBLISH',<<Dup:1,QoS:2,Retain:1>>)-> { Dup, QoS, Retain};
%% parse_flags('PUBREL',<<2#0010>>)-> {ok};
%% parse_flags('SUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags('UNSUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags(_, _)-> throw(invalid_flags). %% [MQTT-2.2.2-2]





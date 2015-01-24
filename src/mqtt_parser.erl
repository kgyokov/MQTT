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
-include("mqtt_packets.hrl").

%% API
-export([read/1, read/3, parse_string/1]).


-record(state, { parse_state } ).
-record(connect_flags, {}).

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
  {ok,Str,Rest};
parse_string(#parse_state{buffer = <<0:16,Rest/binary>>}) ->
  {ok, <<"">>,Rest};
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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Parsing
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
parse_packet(Binary)->
  {ok, packet }
.

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
    ?CONNECT -> 'CONNECT';
    ?CONNACK -> 'CONNACK';
    ?PUBLISH -> 'PUBLISH';
    ?PUBACK -> 'PUBACK';
    ?PUBREC -> 'PUBREC';
    ?PUBREL -> 'PUBREL';
    ?PUBCOMP -> 'PUBCOMP';
    ?UNSUBSCRIBE -> 'SUBSCRIBE';
    ?SUBACK -> 'SUBACK';
    ?UNSUBSCRIBE -> 'UNSUBSCRIBE';
    ?UNSUBACK -> 'UNSUBACK';
    ?PINGREQ -> 'PINGREQ';
    ?PINGRESP -> 'PINGRESP';
    ?DISCONNECT -> 'DISCONNECT';
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

%% MQTT 3.1.2.1 - "The Protocol Name is a UTF-8 encoded string that represents the protocol name “MQTT”, capitalized as shown.
%% The string, its offset and length will not be changed by future versions of the MQTT specification."
parse_specific_type(?CONNECT,
  S = #parse_state{
    buffer =
      <<ProtocolNameLength:16,     %% should be 4 / "MQTT", but according to 3.1.2.1 we may want to give the server
      ProtocolName:32/binary,      %% the option to proceed anyway
      ProtocolLevel:8,
      UsernameFlag:1,PasswordFlag:1,WillRetain:1,WillQoS:2,WillFlag:1,CleanSession:1,_:1,
      KeepAlive:16,

      Payload/binary>>}) ->

  {ok,ClientId, Rest} = parse_string(S#parse_state{buffer=Payload}), %% 3.1.3.1 does not place strict limitations on the ClientId

  {WillDetails,Rest3} = case WillFlag of
                0 ->
                  {undefined,Rest};
                1 ->
                  {ok,WillTopic, Rest1} =  parse_string(S#parse_state{buffer=Rest}),
                  {ok,WillMessage, Rest2} =  parse_string(S#parse_state{buffer=Rest1}),
                  {
                    #will_details{
                      retain = WillRetain,
                      qos = WillQoS,
                      message = WillMessage,
                      topic = WillTopic
                    },
                    Rest2
                  }
    end,
  {Username,Rest4} = case UsernameFlag of
              0 ->
                {undefined,Rest3};
              1 ->
                {ok, U, RestA} =  parse_string(S#parse_state{buffer=Rest3}),
                {U,RestA}
  end,
  {Password,_} = case PasswordFlag of
               0 ->
                 {undefined,Rest4};
               1 ->
                 {ok, P, RestB} =  parse_string(S#parse_state{buffer=Rest4}),
                 {P,RestB}
  end,
  #'CONNECT'{
    protocol_name = ProtocolName,
    protocol_version = ProtocolLevel,
    keep_alive = KeepAlive,
    clean_session = CleanSession,
    client_id = ClientId,
    will = WillDetails,
    username = Username,
    password = Password
  }
;

parse_specific_type('CONNECT', S) ->
  parse_specific_type('CONNECT', read(S))
;

%%%%%%%%%%%%%%
%% PING
%%%%%%%%%%%%%%
parse_specific_type('PINGREQ',<<>>) ->
  #'PINGREQ'{};

%%%%%%%%%%%%%%
%% SUBSCRIBE
%%%%%%%%%%%%%%
parse_specific_type('SUBSCRIBE',<<Rest>>) ->
  #'SUBSCRIBE'{}
.

%% parse_specific_type('CONNECT', S) when byte_size(S#parse_state.buffer) < 10 ->
%%   parse_specific_type('CONNECT', read(S))
%% ;
%% parse_specific_type('CONNECT', _) ->
%%   throw(malformed_packet)
%% .

parse_maybe_will_details(_WillFlag = 0,_,_,#parse_state{buffer=Rest})->
      {undefined,Rest};

parse_maybe_will_details(_WillFlag = 1,WillRetain,WillQoS,S#parse_state{buffer=Rest})->
      {ok,WillTopic, Rest1} =  parse_string(S#parse_state{buffer=Rest}),
      {ok,WillMessage, Rest2} =  parse_string(S#parse_state{buffer=Rest1}),
      {
        #will_details{
          retain = WillRetain,
          qos = WillQoS,
          message = WillMessage,
          topic = WillTopic
        },
        Rest2
      }
.

add_optional_str_field({OptionalFields = #{}, S}, Key, Flag) ->
  if Flag =:= 1 ->
    {ok,Value,Rest} = parse_string(S),
    { maps:put(Key,Value,OptionalFields), S#parse_state{buffer = Rest}};
    Flag =:= 0 -> {OptionalFields, S}
  end
.

%% parse_flags('PUBLISH',<<Dup:1,QoS:2,Retain:1>>)-> { Dup, QoS, Retain};
%% parse_flags('PUBREL',<<2#0010>>)-> {ok};
%% parse_flags('SUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags('UNSUBSCRIBE',<<2#0010>>)-> {ok};
%% parse_flags(_, _)-> throw(invalid_flags). %% [MQTT-2.2.2-2]





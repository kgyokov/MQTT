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
-export([read/1, read_limit/2, read/3, parse_string/1, parse_variable_length/1, parse_packet/1]).


%%
%%
%% Communication Adaptors
%%
%%

read_limit(#parse_state { max_buffer_size =  MaxBufferSize },  ExpectedBufferSize)
  when ExpectedBufferSize >  MaxBufferSize ->
  throw({error, buffer_overflow })
;

read_limit( #parse_state {buffer =  Buffer}, ExpectedBufferSize) when ExpectedBufferSize >=  byte_size(Buffer) ->
  <<Buffer/binary>>
;


read_limit( S = #parse_state { readfun =  ReadFun,buffer =  Buffer}, ExpectedBufferSize)
  when ExpectedBufferSize < byte_size(Buffer)->
  case ReadFun() of
    {ok,NewFragment} ->
      read_limit( S#parse_state{ buffer = <<Buffer/binary,NewFragment/binary>>}, ExpectedBufferSize); %% append the newly retrieved bytes
    {error,Reason} ->
      throw({error,Reason})
  end.

read(_ReadFun, MaxBufferSize, Buffer) when byte_size(Buffer) > MaxBufferSize  ->
  throw({error, buffer_overflow });
read(ReadFun, _MaxBufferSize, Buffer) ->
  case ReadFun() of
    {ok,NewFragment} ->
      <<Buffer/binary,NewFragment/binary>>; %% append the newly retrieved bytes
    {error,Reason} ->
      throw({error,Reason})
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
parse_string(#parse_state{buffer = <<0:16,Rest/binary>>}) ->
  {ok, <<"">>,Rest} %% empty string
;
parse_string(#parse_state{buffer = <<StrLen:16,Str:StrLen/bytes,Rest/binary>>}) ->
  {ok,Str,Rest}
;
parse_string(S)-> %
  parse_string(read(S)) %% fallthrough case: not enough data in the buffer
.

%%
%% Parses integer using variable length-encoding
%%

parse_variable_length(S) ->
  parse_variable_length(S, 0, 1).

parse_variable_length(S = #parse_state{buffer = <<HasMore:1,Length:7, Rest/binary>>},
    Sum,
    Multiplier)->

  NewSum = Sum + Length * Multiplier,
  if HasMore =:= 1 ->
    parse_variable_length(S#parse_state{buffer = Rest}, NewSum, Multiplier * 128);
    true -> {ok, NewSum, Rest}
  end
;

parse_variable_length(S, Sum, Multiplier) ->
  parse_variable_length(read(S), Sum, Multiplier) %% not enough data in the buffer
.


%% parse_variable_length(ReadFun, <<HasMore:1,Length:7, Rest/binary>>, Sum, Multiplier) ->
%%   NewSum = Sum + Length * Multiplier,
%%   if HasMore =:= 1 ->
%%       parse_variable_length(ReadFun, Rest, NewSum, Multiplier * 128);
%%     true -> {NewSum, Rest}
%%   end;
%%
%% parse_variable_length(ReadFun, Bytes, Sum, Multiplier) ->
%%   parse_variable_length(ReadFun, ReadFun(Bytes), Sum, Multiplier).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Parsing
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
parse_packet(S = #parse_state { buffer = <<Type:4,Flags:4,Rest/binary>>})->
  %% get the remaining length of the packet
  {ok,Length,Rest1} = parse_variable_length(S#parse_state{buffer = Rest}),
  %% READ the remaining length of the packet
  <<RemaningPacket:Length/bytes,NextPacket/binary>> = read_limit(S#parse_state{buffer = Rest1},Length),
  %% parse the entire packet based on type, flags, and all other remaining data
  ParsedPacket = parse_specific_type(Type,Flags,RemaningPacket),
  %% return
  {ParsedPacket, S#parse_state{buffer = NextPacket}}
.

%% MQTT 3.1.2.1 - "The Protocol Name is a UTF-8 encoded string that represents the protocol name “MQTT”, capitalized as shown.
%% The string, its offset and length will not be changed by future versions of the MQTT specification."
parse_specific_type(?CONNECT,
  _Flags,
  S = #parse_state{buffer=
  <<ProtocolNameLen:16,                      %% should be 4 / "MQTT", but according to 3.1.2.1 we may want to give the server
   ProtocolName:ProtocolNameLen/bytes,     %% the option to proceed anyway
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


parse_specific_type(?CONNACK,_Flags,<<0:7,SessionPresent:1,Code:8>>) ->
  #'CONNACK'{
    session_present = SessionPresent,
    return_code = Code
  };

parse_specific_type(?DISCONNECT,_Flags,_S) ->
  #'DISCONNECT'{};

%%%%%%%%%%%%%%
%% PING -- COMPLETE!!!
%%%%%%%%%%%%%%
parse_specific_type(?PINGREQ,_Flags,_S) ->
  #'PINGREQ'{};

parse_specific_type(?PINGRESP,_Flags,_S) ->
  #'PINGRESP'{};

%%%%%%%%%%%%%%
%% PUBLISH -- COMPLETE!!!
%%%%%%%%%%%%%%

parse_specific_type(?PUBLISH,Flags, S) ->
  <<Dup:1,QoS:2,Retain:1>> = Flags,

  %% validate flags - should we do that here? or in the connection process???
  case {Dup,QoS,Retain} of
    {1,0,_}->
      throw({error,invalid_flags});
    _ ->
      ok
  end,

  %#parse_state{buffer = Rest } = S,
  {ok,Topic,Rest1} = parse_variable_length(S),
  {PacketId,Content} =
    if QoS =:= 1; QoS =:= 2 ->
      <<PacketId1:16,Content1/binary>> = Rest1,
      {PacketId1,Content1};
    true ->
      {undefined,Rest1}
    end,

  #'PUBLISH'{
    qos = QoS,
    dup = Dup,
    retain = Retain,
    topic = Topic,
    content = Content,
    packet_id = PacketId
  };

parse_specific_type(?PUBACK,_Flags,#parse_state{buffer = <<PacketId:16>>}) ->
  #'PUBACK'{packet_id = PacketId};

parse_specific_type(?PUBREC,_Flags,#parse_state{buffer = <<PacketId:16>>}) ->
  #'PUBREC'{packet_id = PacketId};

parse_specific_type(?PUBREL,_Flags,#parse_state{buffer = <<PacketId:16>>}) ->
  #'PUBREL'{packet_id = PacketId};

parse_specific_type(?PUBCOMP,_Flags,#parse_state{buffer = <<PacketId:16>>}) ->
  #'PUBCOMP'{packet_id = PacketId};

%%%%%%%%%%%%%%
%% SUBSCRIBE - COMPLETE!!!
%%%%%%%%%%%%%%

parse_specific_type(?SUBSCRIBE,_Flags, #parse_state{buffer = <<PacketId:16, Rest/binary>>}) ->
  Subscriptions = parse_topic_subscriptions(Rest),
  #'SUBSCRIBE'{
    packet_id = PacketId,
    subscriptions = Subscriptions
  };

parse_specific_type(?SUBACK,_Flags, #parse_state{buffer = <<PacketId:16, Rest/binary>>}) ->
  ReturnCodes = [ Code || <<0:6,Code:2>> <- binary_to_list(Rest)],
  #'SUBACK'{
    packet_id = PacketId,
    return_codes = ReturnCodes
  }
;

parse_specific_type(?UNSUBSCRIBE,_Flags, #parse_state{buffer = <<PacketId:16, Rest/binary>>}) ->
  Topics = parse_topics(Rest),
  #'UNSUBSCRIBE'{
    packet_id = PacketId,
    topic_filters = Topics
  }
;

parse_specific_type(?UNSUBACK,_Flags,#parse_state{buffer = <<PacketId:16>>}) ->
  #'UNSUBACK'{packet_id = PacketId}
.

%% parse_specific_type('CONNECT', S) when byte_size(S#parse_state.buffer) < 10 ->
%%   parse_specific_type('CONNECT', read(S))
%% ;
%% parse_specific_type('CONNECT', _) ->
%%   throw(malformed_packet)
%% .


parse_topics(Buffer)->
  parse_topics(Buffer,[])
.

parse_topics(_Buffer = <<>>,Topics)->
  Topics
;
parse_topics(<<TopicLen:16,Topic:TopicLen/bytes,Rest/binary>>,Topics)->
  parse_topics(Rest,[Topic|Topics])
.

parse_topic_subscriptions(Buffer)->
  parse_topic_subscriptions(Buffer,[])
.

parse_topic_subscriptions(_Buffer = <<>>,Subscriptions)->
  Subscriptions
;

parse_topic_subscriptions(<<TopicLen:16,Topic:TopicLen/bytes,0:6,QoS:2,Rest/binary>>,
    Subscriptions)->
  parse_topic_subscriptions(Rest,[{Topic,QoS}|Subscriptions])
.

parse_maybe_will_details(_WillFlag = 0,_,_,#parse_state{buffer=Rest})->
      {undefined,Rest};

parse_maybe_will_details(_WillFlag = 1,WillRetain,WillQoS,S = #parse_state{buffer=Rest})->
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






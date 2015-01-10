%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Dec 2014 9:34 PM
%%%-------------------------------------------------------------------
-module(mqtt_builder).
-author("Kalin").

-include("mqtt_packets.hrl").
-include("mqtt_const.hrl").
%% API
-compile(export_all).
-export([build_packet/1, build_string/1, build_var_length/1]).

build_packet(Packet) ->
  Rest = build_rest(Packet),
  <<(build_packet_type(Packet)):4,(build_flags(Packet))/binary,
  (build_var_length(Rest))/binary,
  Rest/binary>>
.

build_flags(Packet)->
  case Packet of
    #'SUBSCRIBE'{} ->   <<2#0010:4>>;
    #'UNSUBSCRIBE'{} -> <<2#0010:4>>;
    #'PUBLISH' { qos = QoS } when QoS =:= 2#11 ->
      throw(invalid_qos);
    #'PUBLISH' { qos = QoS, dup = Dup, retain = Retain } ->
      <<Dup:1,QoS:2,Retain:1>>
  end
.

build_packet_type(Packet)->
  case Packet of
       #'CONNECT'{} -> ?CONNECT;
       #'CONNACK'{}  -> ?CONNACK;
       #'PUBLISH'{}  -> ?PUBLISH;
       #'PUBACK'{}  -> ?PUBACK;
       #'PUBREC'{}  -> ?PUBREC;
       #'PUBREL'{}  -> ?PUBREL;
       #'PUBCOMP'{}  -> ?PUBCOMP;
       #'SUBSCRIBE'{}  -> ?SUBSCRIBE;
       #'SUBACK'{}  -> ?SUBACK;
       #'UNSUBSCRIBE'{}  -> ?UNSUBSCRIBE;
       #'UNSUBACK'{}  -> ?UNSUBACK;
       #'PINGREQ'{}  -> ?PINGREQ;
       #'PINGRESP'{}  ->?PINGRESP;
       #'DISCONNECT'{}  -> ?DISCONNECT
     end
.


build_rest(#'CONNECT'{
    client_id = ClientId,
    username = Username,
    password = Password,
    protocol_name = ProtocolName,
    protocol_version = ProtocolVersion,
    will_message = WillMessage,
    will_topic = WillTopic,
    will_qos = WillQos,
    will_retain = WillRetain,
    clean_session = CleanSession,
    keep_alive = KeepAlive
})->
  %% Validation
  case {ProtocolName,ProtocolVersion} of
    {"MQTT",4} -> ok;
    _ -> throw(unknown_protocol_and_version)
  end,

 <<
 (build_string(ProtocolName))/binary,
   ProtocolVersion:8,
 %% Flags
  (maybe_flag(Username))/binary,
  (maybe_flag(Password))/binary,
  (maybe_flag(WillRetain))/binary,
  (case WillMessage of
      undefined -> 0;
      _ -> WillQos
    end):2,
  (maybe_flag(WillMessage))/binary,
  (maybe_flag(CleanSession))/binary,
  0:1, %Reserved
  KeepAlive:16,

 %% Payload
  (build_string(ClientId))/binary,
  (maybe_build_string(WillTopic))/binary,
  (maybe_build_string(WillMessage))/binary,
  (maybe_build_string(Username))/binary,
  (maybe_build_string(Password))/binary
 >>;

%% CONNACK
build_rest(#'CONNACK'{ flags = Flags, return_code = ReturnCode})->
   <<
  (Flags#connack_flags.session_present):1,
  ReturnCode:8
  >>;

%% PUBLISH
build_rest(#'PUBLISH'{
  packet_id = PacketId,
  qos = QoS,
  topic = Topic,
  content = Content}) when QoS =:= 1;
                           QoS =:= 2 ->
  <<
  (build_string(Topic))/binary,
  PacketId:16,
  Content/binary
  >>;

build_rest(#'PUBLISH'{
  packet_id = undefined,
  qos = QoS,
  topic = Topic,
  content = Content}) when QoS =:= 1 ->
  <<
  (build_string(Topic))/binary,
  Content/binary
  >>;


build_rest(#'PUBACK'{packet_id = PacketId})->
  <<PacketId:16>>;

build_rest(#'PUBREC'{packet_id = PacketId})->
  <<PacketId:16>>;

build_rest(#'PUBREL'{packet_id = PacketId})->
  <<PacketId:16>>;

build_rest(#'PUBCOMP'{packet_id = PacketId})->
  <<PacketId:16>>;

build_rest(#'SUBSCRIBE'{packet_id = PacketId, subscriptions = Subscriptions})->
  <<
  PacketId:16,
  (list_to_binary(lists:map(fun({Topic,QoS})-> <<(build_string(Topic))/binary,QoS:8>> end, Subscriptions)))/binary
  >>
;

build_rest(#'SUBACK'{packet_id = PacketId, return_codes = ReturnCodes})->
  <<
  PacketId:16,
  (list_to_binary(lists:map(fun(Code)-> <<Code:8>> end, ReturnCodes)))/binary
  >>
;


build_rest(#'UNSUBSCRIBE'{packet_id = PacketId, topic_filters = TopicFilters})->
  <<
  PacketId:16,
  (list_to_binary(lists:map(fun(Filter)-> <<(build_string(Filter))/binary>> end, TopicFilters)))/binary
  >>;

build_rest(#'UNSUBACK'{packet_id = PacketId})->
  <<PacketId:16>>;

build_rest(#'PINGREQ'{})->
  <<>>;

build_rest(#'PINGRESP'{})->
  <<>>;

build_rest(#'DISCONNECT'{})->
  <<>>.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%% HELPERS
%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
maybe_build_string(undefined)->
  <<>>;
maybe_build_string(S)->
  build_string(S).

build_string(S) when is_list(S)->
  build_string(list_to_binary(S));
build_string(<<S/binary>>) ->
  <<(byte_size(S)):16,S/binary>>.

build_var_length(Length)->
  build_var_length(Length,<<>>)
.

build_var_length(_Length,Acc) when byte_size(Acc) >= 4 ->
  throw(variable_length_too_large);

build_var_length(Length,Acc)->
  NextLength = Length bsr 7,
  case NextLength of
    0 -> <<Acc/binary,0:1,Length:7>>;
    _ -> build_var_length(NextLength, <<Acc/binary,1:1,Length:7>>)
  end
.

maybe_flag(undefined)->
  <<0:1>>;
maybe_flag(_)->
  <<1:1>>.
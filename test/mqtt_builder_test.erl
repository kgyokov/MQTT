%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%% Tests building of mqtt packets. The mqtt_builder module is called from within our system.
%%% Therefore it is assumed to be called with correct arguments and is coded in a non-defencive manner.
%%% For this reason we do not test error conditions here.
%%% manner
%%% @end
%%% Created : 16. Dec 2014 10:42 PM
%%%-------------------------------------------------------------------
-module(mqtt_builder_test).
-author("Kalin").

-include("mqtt_packets.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_MODULE, mqtt_builder).

-compile(export_all).

%%-------------------------------------------------------
%% Primitives
%%-------------------------------------------------------

build_variable_length_0_test() ->
  Result = mqtt_builder:build_var_length(0),
  ?assertEqual(Result, <<0:8>>).

build_variable_length_127_test() ->
  Result = mqtt_builder:build_var_length(127),
  ?assertEqual(Result, <<127:8>>).

build_variable_length_128_byte_test() ->
  Result = mqtt_builder:build_var_length(128),
  ?assertEqual(Result, <<128:8,1:8>>).

build_variable_length_2_byte_test() ->
  Result = mqtt_builder:build_var_length(321),
  ?assertEqual(Result, <<193:8,2>>).

build_variable_length_max_test() ->
  Result = mqtt_builder:build_var_length(268435455),
  ?assertEqual(Result, <<16#FFFFFF7F:32>>).

build_variable_length_overflow_test() ->
  ?assertException(throw, variable_length_too_large, mqtt_builder:build_var_length(268435456)).


%%-------------------------------------------------------
%% Packages
%% Based on examples from
%% http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html
%%-------------------------------------------------------

build_CONNACK_test() ->
    ?assertEqual(<<2:4,0:4,
                   2:8,
                   1:8,0:8
                 >>,
                 mqtt_builder:build_packet(#'CONNACK'{return_code = ?CONNACK_ACCEPTED,
                                                      session_present = true})).

build_CONNACK_Unacceptable_Protocol_test() ->
    ?assertEqual(<<2:4,0:4,
                   2:8,
                   0:8,1:8
                 >>,
                 mqtt_builder:build_packet(#'CONNACK'{return_code = ?CONNACK_UNACCEPTABLE_PROTOCOL,
                                                      session_present = true})).

build_CONNACK_Identifier_Rejected_test() ->
    ?assertEqual(<<2:4,0:4,
                   2:8,
                   0:8,2:8
                 >>,
                 mqtt_builder:build_packet(#'CONNACK'{return_code = ?CONNACK_IDENTIFIER_REJECTED,
                                                      session_present = true})).

build_CONNACK_Server_Unavailable_test() ->
    ?assertEqual(<<2:4,0:4,
                   2:8,
                   0:8,3:8
                 >>,
                 mqtt_builder:build_packet(#'CONNACK'{return_code = ?CONNACK_SERVER_UNAVAILABLE,
                                                      session_present = true})).

build_CONNACK_BAD_USERNAME_PASSWORD_test() ->
    ?assertEqual(<<2:4,0:4,
                   2:8,
                   0:8,4:8
                 >>,
                 mqtt_builder:build_packet(#'CONNACK'{return_code = ?CONNACK_BAD_USERNAME_OR_PASSWORD,
                                                      session_present = true})).


build_CONNACK_UNAUTHORIZED_test() ->
    ?assertEqual(<<2:4,0:4,
                   2:8,
                   0:8,5:8
                 >>,
                 mqtt_builder:build_packet(#'CONNACK'{return_code = ?CONNACK_UNAUTHORIZED,
                                                      session_present = true})).


build_PUBLISH_test() ->
    ?assertEqual(<<3:4,1:1,2:2,1:1,
                   11:8,
                   3:16,
                   16#61:8,16#2f:8,16#62:8,
                   10:16,
                   1:16,2:16
                 >>,
                  mqtt_builder:build_packet(#'PUBLISH'{packet_id = 10,
                                                       content = <<1:16,2:16>>,
                                                       dup = true,
                                                       qos = 2,
                                                       retain = true,
                                                       topic = <<"a/b"/utf8>>})).

build_PUBACK_test() ->
    ?assertEqual(<<4:4,0:4,
                   2:8,
                   10:16
                 >>,
                 mqtt_builder:build_packet(#'PUBACK'{packet_id = 10})).


build_PUBREC_test() ->
    ?assertEqual(<<5:4,0:4,
                   2:8,
                   10:16
                 >>,
                 mqtt_builder:build_packet(#'PUBREC'{packet_id = 10})).

build_PUBREL_test() ->
    ?assertEqual(<<6:4,2:4,
                   2:8,
                   10:16
                 >>,
                 mqtt_builder:build_packet(#'PUBREL'{packet_id = 10})).

build_PUBCOMP_test() ->
    ?assertEqual(<<7:4,0:4,
                   2:8,
                   10:16
                 >>,
                 mqtt_builder:build_packet(#'PUBCOMP'{packet_id = 10})).


build_SUBSCRIBE_test() ->
    ?assertEqual(<<8:4,2:4,
                   14:8,
                   10:16,
                   3:16,    16#61:8,16#2f:8,16#62:8,    1:8,
                   3:16,    16#63:8,16#2f:8,16#64:8,    2:8
                 >>,
                 mqtt_builder:build_packet(#'SUBSCRIBE'{
                     packet_id = 10,
                    subscriptions = [
                        {<<"a/b">>, 1},
                        {<<"c/d">>, 2}
                    ]
                 })).

build_SUBACK_test() ->
    ?assertEqual(<<9:4,0:4,
                   5:8,
                   10:16,
                   0:8,2:8,128:8
                 >>,
                 mqtt_builder:build_packet(#'SUBACK'{
                    packet_id = 10,
                    return_codes = [0,2,?SUBSCRIPTION_FAILURE]
                 })).

build_UNSUBSCRIBE_test() ->
    ?assertEqual(<<10:4,2:4,
                   12:8,
                   10:16,
                   3:16,    16#61:8,16#2f:8,16#62:8,
                   3:16,    16#63:8,16#2f:8,16#64:8
                 >>,
                 mqtt_builder:build_packet(#'UNSUBSCRIBE'{
                     packet_id = 10,
                     topic_filters = [ <<"a/b">>, <<"c/d">>]
                 })).


build_UNSUBACK_test() ->
    ?assertEqual(<<11:4,0:4,
                   2:8,
                   10:16
                 >>,
                 mqtt_builder:build_packet(#'UNSUBACK'{packet_id = 10})).

build_PINGREQ_test() ->
    ?assertEqual(<<12:4,0:4,
                   0:8
                 >>,
                 mqtt_builder:build_packet(#'PINGREQ'{})).

build_PINGRESP_test() ->
    ?assertEqual(<<13:4,0:4,
                   0:8
                 >>,
                 mqtt_builder:build_packet(#'PINGRESP'{})).

build_DISCONNECT_test() ->
    ?assertEqual(<<14:4,0:4,
                   0:8
                 >>,
                 mqtt_builder:build_packet(#'DISCONNECT'{})).
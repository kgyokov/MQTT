%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2014 2:14 PM
%%%-------------------------------------------------------------------
-module(mqtt_parser_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").
-include("mqtt_const.hrl").
-include("mqtt_packets.hrl").


-compile([exportall]).

%% -export([parse_string_test/0, parse_string_chunked_test/0, parse_string_chunked_test/0,
%%           parse_string_chunked2_test/0, parse_string_chunked3_test/0,all_tests/0
%% ]).


parse_string_test()->
  String = <<9:16,"123456789">>,
  ReadFun = fun() -> {ok, String } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, <<"123456789">>, <<>>}, mqtt_parser:parse_string(State)).

parse_string_chunked_test()->
  ParseProcess = initialize_parse_process(<<>>, fun mqtt_parser:parse_string/1),

    %% Send string in two separate chunks
  push_fragment(ParseProcess,<<9:16, "12345">>),
  push_fragment(ParseProcess,<<"6789">>),

  ?assertEqual({ok, <<"123456789">>, <<>>},receive_result(ParseProcess))
.

parse_string_chunked2_test()->
  ParseProcess = initialize_parse_process(<<>>,fun mqtt_parser:parse_string/1),

  %% Send string in two separate chunks
  push_fragment(ParseProcess,<<0:8>>),
  push_fragment(ParseProcess,<<9:8>>),
  push_fragment(ParseProcess,<<"12345">>),
  push_fragment(ParseProcess,<<"6789">>),

  ?assertEqual({ok, <<"123456789">>, <<>>} , receive_result(ParseProcess))
.

parse_string_chunked3_test()->
  ParseProcess = initialize_parse_process(<<>>,fun mqtt_parser:parse_string/1),

  %% Send string in two separate chunks
  push_fragment(ParseProcess,<<0:8>>),
  push_fragment(ParseProcess,<<9:8>>),
  push_fragment(ParseProcess,<<"12345">>),
  push_fragment(ParseProcess,<<"6789abc">>),

  ?assertEqual({ok, <<"123456789">>, <<"abc">>}, receive_result(ParseProcess))
.

parse_variable_length_1_byte_test()->
  Length = <<1:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 1, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_1_byte_2_test()->
  Length = <<127:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 127, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_2_bytes_test()->
  Length = <<193:8,2:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 321, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_3_bytes_test()->
  Length = <<193:8,2:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 321, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_4_bytes_test()->
  Length = <<255:8,255:8,255:8,127:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 268435455, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_chunked_test()->
  ParseProcess = initialize_parse_process(<<>>, fun mqtt_parser:parse_variable_length/1),
  push_fragment(ParseProcess,<<255:8,255:8>>),
  push_fragment(ParseProcess,<<255:8,127:8>>),

  ?assertEqual({ok, 268435455, <<>>}, receive_result(ParseProcess)).


%%
%%
%% Packets
%%
%%

parse_CONNECT_1_test()->
  OriginalPacket = #'CONNECT'{
  clean_session = true,
  client_id = <<"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ">>,
  keep_alive = 600,
  password = list_to_binary([ <<"P">> || _  <- lists:seq(1,65535)]),
  username = list_to_binary([ <<"U">> || _  <- lists:seq(1,65535)]),
  protocol_name = <<"MQTT">>,
  protocol_version = 4,
  will = #will_details {message = <<"WILL_MESSAGE">>, qos = 2, topic = <<"TOPIC">>, retain = true}
  },

  test_packet(OriginalPacket)
.

parse_CONNACK_1_test()->
  OriginalPacket = #'CONNACK'{return_code = 0, session_present = true},
  test_packet(OriginalPacket)
.

parse_CONNACK_2_test()->
  OriginalPacket = #'CONNACK'{return_code = 0, session_present = false},
  test_packet(OriginalPacket)
.


parse_DISCONNECT_test()->
  OriginalPacket = #'DISCONNECT'{},
  test_packet(OriginalPacket)
.

parse_PINGREG_test()->
  OriginalPacket = #'PINGREQ'{},
  test_packet(OriginalPacket)
.

parse_PINGRESP_test()->
  OriginalPacket = #'PINGRESP'{},
  test_packet(OriginalPacket)
.


parse_PUBLISH_QoS_0_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = undefined,
    qos = 0,
    dup = 0,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  test_packet(OriginalPacket)
.

parse_PUBLISH_QoS_0_invalid_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = undefined,
    qos = 0,
    dup = 1,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  ?assertException(throw,{error,_},test_packet(OriginalPacket))
.

parse_PUBLISH_QoS_1_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = 1234,
    qos = 1,
    dup = 1,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  test_packet(OriginalPacket)
.

parse_PUBLISH_QoS_1_1_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = 1234,
    qos = 1,
    dup = 0,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  test_packet(OriginalPacket)
.

parse_PUBLISH_QoS_2_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = 1234,
    qos = 1,
    dup = 1,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  test_packet(OriginalPacket)
.

parse_PUBLISH_QoS_2_1_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = 1234,
    qos = 1,
    dup = 0,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  test_packet(OriginalPacket)
.

parse_PUBLISH_QoS_1_invalid_packet_id_test()->
  OriginalPacket = #'PUBLISH'{
    packet_id = undefined,
    qos = 1,
    dup = 0,
    retain = 1,
    topic = <<"TOPIC1">>,
    content = <<"CONTENT">>
  },
  ?assertException(_,_,test_packet(OriginalPacket))
.

parse_PUBLISH_empty__test()->
  OriginalPacket = #'SUBSCRIBE'{
    packet_id = 1234,
    subscriptions = []
  },
  test_packet(OriginalPacket)
.

parse_SUBSCRIBE_test()->
  OriginalPacket = #'SUBSCRIBE'{
    packet_id = 1234,
   subscriptions = [
     {<<"SUB1">>, 0},
     {<<"SUB2">>, 2},
     {<<"SUB3">>, 1}
   ]
  },
  test_packet(OriginalPacket)
.

parse_SUBSCRIBE_empty__test()->
  OriginalPacket = #'SUBSCRIBE'{
    packet_id = 1234,
    subscriptions = []
  },
  test_packet(OriginalPacket)
.

parse_SUBACK_test()->
  OriginalPacket = #'SUBACK'{
    packet_id = 1234,
    return_codes = [ 1,3,2]
  },
  test_packet(OriginalPacket)
.

parse_SUBACK_empty__test()->
  OriginalPacket = #'SUBACK'{
    packet_id = 1234,
    return_codes = []
  },
  test_packet(OriginalPacket)
.

parse_UNSUBSCRIBE_test()->
  OriginalPacket = #'UNSUBSCRIBE'{
    packet_id = 1234,
    topic_filters = [ <<"SUB1">>, <<"SUB2">>, <<"SUB3">> ]
  },
  test_packet(OriginalPacket)
.

parse_UNSUBSCRIBE_empty__test()->
  OriginalPacket = #'UNSUBSCRIBE'{
    packet_id = 1234,
    topic_filters = []
  },
  test_packet(OriginalPacket)
.

parse_UNSUBACK_test()->
  OriginalPacket = #'UNSUBACK'{packet_id = 1234},
  test_packet(OriginalPacket)
.





%%
%%
%% Test Utilities
%%
%%

test_packet(OriginalPacket)->
  Binary = mqtt_builder:build_packet(OriginalPacket),
  S = #parse_state{buffer = Binary, max_buffer_size = 100000, readfun = undefined},
  {ParsedPacket,_NewState} = mqtt_parser:parse_packet(S),
  ?assertEqual(OriginalPacket,ParsedPacket)
.

initialize_parse_process(StartBuffer, Fun)->
  ReadFun = fun() -> receive Fragment -> {ok, Fragment } after 1000 -> {error, timeout} end end,
  State = #parse_state { buffer = StartBuffer, readfun = ReadFun, max_buffer_size = 1000000 },
  Self = self(),
  spawn(fun() -> Self! { self(), Fun(State)} end).

push_fragment(ParseProcess,NewFragment)->
  ParseProcess! NewFragment.


receive_result(ParseProcess)->
  receive
    {ParseProcess, Response} ->
      Response
  after 1000 ->
    throw(unresponsive)
  end.


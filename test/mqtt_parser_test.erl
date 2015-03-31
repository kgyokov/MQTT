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
-include("mqtt_parsing.hrl").
-include("mqtt_packets.hrl").


-compile([exportall]).

%% ======================================================
%% Parse Primitives
%% ======================================================

parse_string_test()->
    String = <<9:16,"123456789">>,
    ReadFun = fun(_) -> {ok, String } end,
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
    ReadFun = fun(_) -> {ok, Length } end,
    State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
    ?assertEqual({ok, 1, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_1_byte_2_test()->
    Length = <<127:8>>,
    ReadFun = fun(_) -> {ok, Length } end,
    State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
    ?assertEqual({ok, 127, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_2_bytes_test()->
    Length = <<193:8,2:8>>,
    ReadFun = fun(_) -> {ok, Length } end,
    State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
    ?assertEqual({ok, 321, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_3_bytes_test()->
    Length = <<193:8,2:8>>,
    ReadFun = fun(_) -> {ok, Length } end,
    State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
    ?assertEqual({ok, 321, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_4_bytes_test()->
    Length = <<255:8,255:8,255:8,127:8>>,
    ReadFun = fun(_) -> {ok, Length } end,
    State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
    ?assertEqual({ok, 268435455, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_chunked_test()->
    ParseProcess = initialize_parse_process(<<>>, fun mqtt_parser:parse_variable_length/1),
    push_fragment(ParseProcess,<<255:8,255:8>>),
    push_fragment(ParseProcess,<<255:8,127:8>>),

    ?assertEqual({ok, 268435455, <<>>}, receive_result(ParseProcess)).


%% ======================================================
%% Parse Packets
%% ======================================================

%% ------------------------------------------------------
%% CONNECT
%% ------------------------------------------------------

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

    test_packet(OriginalPacket).


%% ------------------------------------------------------
%% CONNACK
%% ------------------------------------------------------
parse_CONNACK_valid_test_() ->
    [
        fun() ->
            parse_CONNACK(?CONNACK_ACCEPTED,true)
        end,
        fun() ->
            parse_CONNACK(?CONNACK_ACCEPTED,false)
        end,
        fun() ->
            parse_CONNACK(?CONNACK_IDENTIFIER_REJECTED,false)
        end,
        fun() ->
            parse_CONNACK(?CONNACK_SERVER_UNAVAILABLE,false)
        end,
        fun() ->
            parse_CONNACK(?CONNACK_UNACCEPTABLE_PROTOCOL,false)
        end,
        fun() ->
            parse_CONNACK(?CONNACK_UNAUTHORIZED,false)
        end,
        fun() ->
            parse_CONNACK(?CONNACK_BAD_USERNAME_OR_PASSWORD,false)
        end
    ].


parse_CONNACK_invalid_test_() ->
    [
        fun() ->
            parse_CONNACK_invalid(?CONNACK_IDENTIFIER_REJECTED,true)
        end,
        fun() ->
            parse_CONNACK_invalid(?CONNACK_SERVER_UNAVAILABLE,true)
        end,
        fun() ->
            parse_CONNACK_invalid(?CONNACK_UNACCEPTABLE_PROTOCOL,true)
        end,
        fun() ->
            parse_CONNACK_invalid(?CONNACK_UNAUTHORIZED,true)
        end,
        fun() ->
            parse_CONNACK_invalid(?CONNACK_BAD_USERNAME_OR_PASSWORD,true)
        end
    ].

parse_CONNACK(RetCode, SessPresent) ->
    OriginalPacket = #'CONNACK'{return_code = RetCode, session_present = SessPresent},
    test_packet(OriginalPacket).

parse_CONNACK_invalid(RetCode, SessPresent) ->
    Packet = <<2:4,0:4,2:8,0:7,(case SessPresent of true -> 1; false ->0 end):1,RetCode:8>>,
    test_binary_packet_for_error(Packet).

%% ------------------------------------------------------
%% PUBLISH
%% ------------------------------------------------------

parse_PUBLISH_QoS_0_test() ->
    OriginalPacket = #'PUBLISH'{
        packet_id = undefined,
        qos = 0,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    test_packet(OriginalPacket).

%% [MQTT-3.3.1-2]
parse_PUBLISH_QoS_0_Dup_test() ->
    OriginalPacket = #'PUBLISH'{
        packet_id = undefined,
        qos = 0,
        dup = true,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    test_packetfor_error(OriginalPacket,invalid_flags).

parse_PUBLISH_QoS_1_Dup_test() ->
    OriginalPacket = #'PUBLISH'{
        packet_id = 1234,
        qos = 1,
        dup = true,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    test_packet(OriginalPacket).

parse_PUBLISH_QoS_1_NoDup_test() ->
    OriginalPacket = #'PUBLISH'{
        packet_id = 1234,
        qos = 1,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    test_packet(OriginalPacket).

parse_PUBLISH_QoS_2_NoDup_test()->
    OriginalPacket = #'PUBLISH'{
        packet_id = 1234,
        qos = 2,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    test_packet(OriginalPacket).

parse_PUBLISH_QoS_2_Dup_test()->
    OriginalPacket = #'PUBLISH'{
        packet_id = 1234,
        qos = 2,
        dup = true,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    test_packet(OriginalPacket).

%% 3.3.2.2
parse_PUBLISH_QoS_1_invalid_packet_id_test()->
    OriginalPacket = #'PUBLISH'{
        packet_id = undefined,
        qos = 1,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    ?assertException(error,_,test_packet(OriginalPacket)).

%% 3.3.2.2
parse_PUBLISH_QoS_2_invalid_packet_id_test()->
    OriginalPacket = #'PUBLISH'{
        packet_id = undefined,
        qos = 2,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },
    ?assertException(error,_,test_packet(OriginalPacket)).

%% [MQTT-3.3.1-10]
parse_PUBLISH_empty_test() ->
    OriginalPacket = #'PUBLISH'{
        packet_id = undefined,
        qos = 2,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<>>
    },
    ?assertException(error,_,test_packet(OriginalPacket)).


%% ------------------------------------------------------
%% SUBSCRIBE
%% ------------------------------------------------------

parse_SUBSCRIBE_test() ->
    OriginalPacket = #'SUBSCRIBE'{
        packet_id = 1234,
        subscriptions = [
            {<<"SUB1">>, 0},
            {<<"SUB2">>, 2},
            {<<"SUB3">>, 1}
        ]
    },
    test_packet(OriginalPacket).

parse_SUBSCRIBE_empty__test()->
    OriginalPacket = #'SUBSCRIBE'{
        packet_id = 1234,
        subscriptions = []
    },
    test_packet(OriginalPacket).


%% ------------------------------------------------------
%% SUBACK
%% ------------------------------------------------------

parse_SUBACK_test()->
    OriginalPacket = #'SUBACK'{
        packet_id = 1234,
        return_codes = [1,3,2]
    },
    test_packet(OriginalPacket).

parse_SUBACK_empty__test() ->
    OriginalPacket = #'SUBACK'{
        packet_id = 1234,
        return_codes = []
    },
    test_packet(OriginalPacket).


%% ------------------------------------------------------
%% UNSUBSCRIBE
%% ------------------------------------------------------

parse_UNSUBSCRIBE_test() ->
    OriginalPacket = #'UNSUBSCRIBE'{
        packet_id = 1234,
        topic_filters = [ <<"SUB1">>, <<"SUB2">>, <<"SUB3">> ]
    },
    test_packet(OriginalPacket).

parse_UNSUBSCRIBE_empty__test() ->
    OriginalPacket = #'UNSUBSCRIBE'{
        packet_id = 1234,
        topic_filters = []
    },
    test_packet(OriginalPacket).


%% ------------------------------------------------------
%% UNSUBACK
%% ------------------------------------------------------

parse_UNSUBACK_test() ->
    OriginalPacket = #'UNSUBACK'{packet_id = 1234},
    test_packet(OriginalPacket).

%% ------------------------------------------------------
%% PINGS
%% ------------------------------------------------------

parse_PINGREG_test() ->
    OriginalPacket = #'PINGREQ'{},
    test_packet(OriginalPacket).

parse_PINGRESP_test() ->
    OriginalPacket = #'PINGRESP'{},
    test_packet(OriginalPacket).

parse_DISCONNECT_test() ->
    OriginalPacket = #'DISCONNECT'{},
    test_packet(OriginalPacket).


%%-----------------------------------------------------------
%% Misc. Tests
%%-----------------------------------------------------------

parse_chunked_packet_test()->
    OriginalPacket = #'UNSUBSCRIBE'{
        packet_id = 1234,
        topic_filters = [ <<"SUB1">>, <<"SUB2">>, <<"SUB3">> ]
    },

    Binary = mqtt_builder:build_packet(OriginalPacket),
    HalfLen = byte_size(Binary) div 2,
    <<Part1:HalfLen/binary,Part2/binary>> = Binary,

    ParseProcess = initialize_parse_process(<<>>,fun mqtt_parser:parse_packet/1),
    push_fragment(ParseProcess,Part1),
    push_fragment(ParseProcess,Part2),

    {ok, ParsedPacket,_S} = receive_result(ParseProcess),
    ?assertEqual(OriginalPacket,ParsedPacket).


parse_2_consecutive_packets_test()->
    OriginalPacket1 = #'UNSUBSCRIBE'{
        packet_id = 1234,
        topic_filters = [ <<"SUB1">>, <<"SUB2">>, <<"SUB3">> ]
    },
    OriginalPacket2 = #'PUBLISH'{
        packet_id = undefined,
        qos = 0,
        dup = false,
        retain = true,
        topic = <<"TOPIC1">>,
        content = <<"CONTENT">>
    },

    Binary = <<(mqtt_builder:build_packet(OriginalPacket1))/binary,
               (mqtt_builder:build_packet(OriginalPacket2))/binary>>,
    S = #parse_state{buffer = Binary, max_buffer_size = 100000, readfun = undefined},
    {ok,ParsedPacket1,S1} = mqtt_parser:parse_packet(S),
    {ok,ParsedPacket2,_S2} = mqtt_parser:parse_packet(S1),

    ?assertEqual(OriginalPacket1,ParsedPacket1),
    ?assertEqual(OriginalPacket2,ParsedPacket2).


parse_incomplete_packet_test() ->
    IncompletePacket = <<8:4,2:4,
                         13:8,   %% we actually need 16 bytes
                         10:16,
                         3:16,    16#61:8,16#2f:8,16#62:8,    1:8,
                         5:16,    16#63:8,16#2f:8,16#64:8    %% 3 bytes missing
                        >>,

    test_binary_packet_for_error(IncompletePacket).


%%-----------------------------------------------------------
%% Test Utilities
%%-----------------------------------------------------------

test_packet(OriginalPacket) ->
    Binary = mqtt_builder:build_packet(OriginalPacket),
    S = #parse_state{buffer = Binary, max_buffer_size = 100000, readfun = undefined},
    ?assertMatch({ok, OriginalPacket,_S1}, mqtt_parser:parse_packet(S)).

test_binary_packet_for_error(Binary) ->
    S = #parse_state{buffer = Binary, max_buffer_size = 100000, readfun = undefined},
    ?assertMatch({error, _}, mqtt_parser:parse_packet(S)).

test_packetfor_error(OriginalPacket,Reason) ->
    Binary = mqtt_builder:build_packet(OriginalPacket),
    S = #parse_state{buffer = Binary, max_buffer_size = 100000, readfun = undefined},
    ?assertMatch({error, Reason}, mqtt_parser:parse_packet(S)).

initialize_parse_process(StartBuffer, Fun) ->
    ReadFun = fun(_) -> receive Fragment -> {ok, Fragment } after 1000 -> {error, timeout} end end,
    State = #parse_state { buffer = StartBuffer, readfun = ReadFun, max_buffer_size = 1000000 },
    Self = self(),
    spawn(fun() -> Self! { self(), Fun(State)} end).

push_fragment(ParseProcess,NewFragment) ->
    ParseProcess! NewFragment.


receive_result(ParseProcess)->
    receive
        {ParseProcess, Response} ->
            Response
    after 1000 ->
        throw(unresponsive)
    end.


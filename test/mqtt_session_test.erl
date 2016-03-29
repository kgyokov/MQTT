%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Mar 2016 10:04 PM
%%%-------------------------------------------------------------------
-module(mqtt_session_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").

-include("mqtt_internal_msgs.hrl").


setup_session() ->
    TestPacket = #packet{content = <<1>>,
                         retain = false,
                         topic = <<"TestFilter">>,
                         ref = {q,1},
                         qos = ?QOS_1},
    [mqtt_session:new(2),TestPacket].

msg_test_() ->
    {
        foreach, fun setup_session/0, fun(_) -> ok end,
        [
            %% Buffer
            fun push_from_unknown_filter_drops_packet/1,
            fun push_on_no_subscriptions_drops_packet/1,
            fun push_of_duplicate_packet_drops_packet/1,
            fun push_from_matching_filter_emits_packet/1,

            fun push_emits_packet_w_correct_values/1,

            fun push_on_half_full_buffer_emits_packet/1,
            fun push_on_full_buffer_queues_packet/1,
            fun push_on_empty_buffer_emits_packet/1,

            %% Session
            fun ack_on_full_session_emits_next_packet/1
%%            fun push_on_empty_session_emits_packet_2/1
%%            fun msg_in_flight_qos1/1,
%%            fun msg_in_flight_qos1_flow_complete/1,
%%            fun msg_in_flight_qos2_phase1/1,
%%            fun msg_in_flight_qos2_phase2/1,
%%            fun msg_in_flight_qos2_flow_complete/1
        ]
    }.

push_on_no_subscriptions_drops_packet([SO,Packet]) ->
    Result = mqtt_session:push(<<"TestFilter">>,Packet,SO),
    ?_assertMatch({[],SO},Result).

push_from_unknown_filter_drops_packet([SO,Packet]) ->
    Sub = {<<"TestFilter">>,?QOS_1,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    Result = mqtt_session:push(<<"Not_TestFilter">>,Packet,SO1),
    ?_assertMatch({[],SO1},Result).

push_of_duplicate_packet_drops_packet([SO,Packet]) ->
    Sub = {<<"TestFilter">>,?QOS_1,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    {_,SO2} = mqtt_session:push(<<"Not_TestFilter">>,Packet,SO1),
    Result = mqtt_session:push(<<"Not_TestFilter">>,Packet,SO2),
    ?_assertMatch({[],SO1},Result).

push_from_matching_filter_emits_packet([SO,Packet]) ->
    Sub = {<<"TestFilter/+">>,?QOS_1,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    Packet1 = Packet#packet{topic = <<"TestFilter/A">>},
    Result = mqtt_session:push(<<"TestFilter/+">>,Packet1,SO1),
    ?_assertMatch({[#'PUBLISH'{}],_SO2},Result).

push_on_empty_buffer_emits_packet([SO,Packet]) ->
    Sub = {<<"TestFilter">>,?QOS_2,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    Result = mqtt_session:push(<<"TestFilter">>,Packet,SO1),
    ?_assertMatch(
        {[#'PUBLISH'{}],_SO2},
        Result).

push_on_half_full_buffer_emits_packet([SO,Packet1]) ->
    Sub = {<<"TestFilter">>,?QOS_2,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    Packet2 = Packet1#packet{content = <<2>>, ref = {q,2}},
    {_,SO2} = mqtt_session:push(<<"TestFilter">>,Packet1,SO1),
    Result  = mqtt_session:push(<<"TestFilter">>,Packet2,SO2),
    ?_assertMatch(
        {[#'PUBLISH'{content = <<2>>}],_SO3},
        Result).

push_on_full_buffer_queues_packet([SO,Packet1]) ->
    Sub = {<<"TestFilter">>,?QOS_2,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    Packet2 = Packet1#packet{content = <<2>>, ref = {q,2}},
    Packet3 = Packet2#packet{content = <<3>>, ref = {q,3}},
    {_,SO2} = mqtt_session:push(<<"TestFilter">>,Packet1,SO1),
    {_,SO3} = mqtt_session:push(<<"TestFilter">>,Packet2,SO2),
    Result  = mqtt_session:push(<<"TestFilter">>,Packet3,SO3),
    ?_assertMatch(
        {[],_SO4},
        Result).

%% =====================================================================================
%% == SESSION ==
%% =====================================================================================

ack_on_full_session_emits_next_packet([SO,Packet1]) ->
    Sub = {<<"TestFilter">>,?QOS_2,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),

    Packet2 = Packet1#packet{content = <<2>>, ref = {q,2}},
    Packet3 = Packet2#packet{content = <<3>>, ref = {q,3}},

    {[Pub1],SO2} = mqtt_session:push(<<"TestFilter">>,Packet1,SO1),
    {_,SO3}    = mqtt_session:push(<<"TestFilter">>,Packet2,SO2),
    {_,SO4}    = mqtt_session:push(<<"TestFilter">>,Packet3,SO3),

    Result = mqtt_session:pub_ack(Pub1#'PUBLISH'.packet_id,SO4),
    ?_assertMatch(
        {[#'PUBLISH'{content = <<3>>}],_SO5},
        Result).

rec_on_full_session_emits_next_packet([SO,Packet1]) ->
    Sub = {<<"TestFilter">>,?QOS_2,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),

    Packet2 = Packet1#packet{content = <<2>>, ref = {q,2}},
    Packet3 = Packet2#packet{content = <<3>>, ref = {q,3}},

    {Pub1,SO2} = mqtt_session:push(<<"TestFilter">>,Packet1,SO1),
    {_,SO3}    = mqtt_session:push(<<"TestFilter">>,Packet2,SO2),
    {_,SO4}    = mqtt_session:push(<<"TestFilter">>,Packet3,SO3),

    Result = mqtt_session:pub_ack(Pub1#'PUBLISH'.packet_id,SO4),
    ?_assertMatch(
        {[#'PUBLISH'{content = <<3>>}],_SO5},
        Result).

push_emits_packet_w_correct_values([SO,Packet]) ->
    Sub = {<<"TestFilter">>,?QOS_2,mqtt_seq:bottom()},
    SO1 = mqtt_session:subscribe([Sub],SO),
    Result= mqtt_session:push(<<"TestFilter">>,Packet,SO1),
    ?_assertMatch(
        {[#'PUBLISH'{qos = ?QOS_1,
                     topic = <<"TestFilter">>,
                     retain = false,
                     dup = false,
                     packet_id = _,
                     content = <<1>>}],
            _SO2},
        Result).

fill_session(SO1,Packet1) ->
    Packet2 = Packet1#packet{content = <<"Packet2">>, ref = {q,2}},
    Packet3 = Packet2#packet{content = <<"Packet2">>, ref = {q,3}},
    {_,SO2} = mqtt_session:push(<<"TestFilter">>,Packet1,SO1),
    {_,SO3} = mqtt_session:push(<<"TestFilter">>,Packet2,SO2),
    {_,SO4} = mqtt_session:push(<<"TestFilter">>,Packet3,SO3),
    SO4.



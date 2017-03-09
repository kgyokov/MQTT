%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Apr 2016 1:01 AM
%%%-------------------------------------------------------------------
-module(mqtt_sub_state_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").
-include("test_packages.hrl").

-define(assertPackagesEqual(LP1,LP2), ?assertEqual(
    [P#packet{seq = '_'} || P <- LP1],
    [P#packet{seq = '_'} || P <- LP2])
).

-define(ACCUMULATORS,accumulator_gb_tree).
-define(MONOID,monoid_packets).

take_from_empty_queue_returns_empty_list_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_0,Q),
    {Taken,_} = mqtt_sub_state:take(5,Q,Sub),
    ?assertPackagesEqual([],Taken).

take_0_returns_empty_list_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_0,Q),
    Q1 = push_packets(?QOS_0_PACKET_TOPIC1,Q),
    {Taken,_} = mqtt_sub_state:take(0,Q1,Sub),
    ?assertPackagesEqual([],Taken).

take_immediately_after_new_subscription_returns_empty_list_test() ->
    Q = new_queue(0),
    Q1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?QOS_1_PACKET_TOPIC1],Q),

    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q1),
    {Taken,_} = mqtt_sub_state:take(10,Q1,Sub),
    ?assertPackagesEqual([],Taken).

subscription_starts_w_only_and_all_retained_messages_test() ->
    Q = new_queue(0),
    Q1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?QOS_1_PACKET_TOPIC1],Q),

    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q1),
    {Taken,_} = mqtt_sub_state:take(10,Q1,Sub),
    ?assertPackagesEqual([?RETAINED_PACKET_TOPIC1],Taken).

subscription_proceeeds_when_there_are_new_messages_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q),
    {_,Sub1} = mqtt_sub_state:take(10,Q,Sub),

    Q1 = push_packets([?QOS_0_PACKET_TOPIC1],Q),

    {Taken,_} = mqtt_sub_state:take_any(Q1,Sub1),
    ?assertPackagesEqual([?QOS_0_PACKET_TOPIC1],Taken).

only_requested_umber_of_messages_are_sent_messages_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q),
    {_,Sub1} = mqtt_sub_state:take(2,Q,Sub),

    Q1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?QOS_1_PACKET_TOPIC1],Q),

    {Taken,_} = mqtt_sub_state:take_any(Q1,Sub1),
    ?assertPackagesEqual([?QOS_0_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC1],Taken).

requested_messages_number_is_aggregated_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q),
    {_,Sub1} = mqtt_sub_state:take(2,Q,Sub),
    {_,Sub2} = mqtt_sub_state:take(1,Q,Sub1),

    Q1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?QOS_1_PACKET_TOPIC1],Q),

    {Taken,_} = mqtt_sub_state:take_any(Q1,Sub2),
    ?assertPackagesEqual([?QOS_0_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC1,?QOS_1_PACKET_TOPIC1],Taken).

iterator_is_stoppped_and_then_resumed_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q),
    {_,Sub1} = mqtt_sub_state:take(2,Q,Sub),

    Q1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?QOS_1_PACKET_TOPIC1],Q),

    {Taken,_} = mqtt_sub_state:take(1,Q1,Sub1),
    ?assertPackagesEqual([?QOS_0_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC1,?QOS_1_PACKET_TOPIC1],Taken).

%%
%% We need to ensure that we do not see messages from the future
%%
retained_messages_are_resent_starting_from_most_recent_seen_messages_test() ->
    Q = new_queue(0),
    {_,Sub} = mqtt_sub_state:new(1,?QOS_1,Q),

    Q1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2,
        ?RETAINED_PACKET_TOPIC2
        ],Q),

    {Taken,Sub1} = mqtt_sub_state:take(2,Q1,Sub),
    {_,Sub2} = mqtt_sub_state:reset(?QOS_1,Q1,Sub1),
    {Taken1,_} = mqtt_sub_state:take(2,Q1,Sub2),
    ?assertPackagesEqual([
        ?QOS_0_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2
    ],Taken ++ Taken1).


push_packets(Packets) ->
    push_packets(Packets,new_queue(0)).

push_packets(Packets,SQ) when is_list(Packets) ->
    lists:foldl(fun shared_queue:pushr/2,SQ,Packets);

push_packets(Packets,SQ) ->
    shared_queue:pushr(Packets,SQ).

new_queue(Seq) ->
    shared_queue:new(Seq,?MONOID,?ACCUMULATORS).
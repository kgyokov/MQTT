%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Mar 2017 8:03 PM
%%%-------------------------------------------------------------------
-module(shared_queue_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").
-include("test_packages.hrl").

-define(ACCUMULATORS,accumulator_gb_tree).
-define(MONOID,monoid_sequence).


no_packets_test() ->
    SQ = new_queue(0),

    assert_shared_queue_contents(
        [],
        [],
        [],
        SQ
    ).

single_non_retained_packet_test() ->

    SQ1 = push_packets(?QOS_0_PACKET_TOPIC1),

    assert_shared_queue_contents(
        [],
        [],
        [?QOS_0_PACKET_TOPIC1],
        SQ1
    ).

multiple_non_retained_packet_test() ->

    SQ1 = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2]),

    assert_shared_queue_contents(
        [],
        [],
        [?QOS_0_PACKET_TOPIC1,?QOS_0_PACKET_TOPIC2],
        SQ1
    ).

single_retained_packet_test() ->
    SQ1 = push_packets(?RETAINED_PACKET_TOPIC1),

    assert_shared_queue_contents(
        [],
        [?RETAINED_PACKET_TOPIC1],
        [?RETAINED_PACKET_TOPIC1],
        SQ1
    ).

multiple_retained_packet_test() ->
    SQ1 = push_packets([
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC2]),

    assert_shared_queue_contents(
        [],
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC2],
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC2],
        SQ1
    ).

double_retained_packet_test() ->
    SQ1 = push_packets([
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET2_TOPIC1]),

    assert_shared_queue_contents(
        [],
        [?RETAINED_PACKET2_TOPIC1],
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET2_TOPIC1],
        SQ1
    ).

clear_retained_packet_test() ->
    SQ2 = push_packets([
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_CLEAR_TOPIC1]),

    assert_shared_queue_contents(
        [],
        [],
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET_CLEAR_TOPIC1],
        SQ2
    ).

take_in_middle_of_queue_test() ->
    SQ = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2,
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC2],
        new_queue(0)),

    SQ1 = shared_queue:take(2,1,SQ),

    assert_shared_queue_contents(
        [],
        [?RETAINED_PACKET_TOPIC1],
        [?RETAINED_PACKET_TOPIC1],
        SQ1
    ).

take_end_of_queue_test() ->
    SQ = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2,
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC2],
        new_queue(0)),

    SQ1 = shared_queue:take(3,1,SQ),

    assert_shared_queue_contents(
        [?RETAINED_PACKET_TOPIC1],
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC2],
        [?RETAINED_PACKET_TOPIC2],
        SQ1
    ).

take_beyond_end_of_queue_test() ->
    SQ = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2,
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC2],
        new_queue(0)),

    SQ1 = shared_queue:take(4,1,SQ),

    assert_shared_queue_contents(
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC2],
        [?RETAINED_PACKET_TOPIC1,?RETAINED_PACKET_TOPIC2],
        [],
        SQ1
    ).

take_before_beginning_of_queue_test() ->
    SQ = push_packets([
        ?QOS_0_PACKET_TOPIC1,
        ?QOS_0_PACKET_TOPIC2,
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET_TOPIC2],
        new_queue(1)),

    SQ1 = shared_queue:take(0,1,SQ),

    assert_shared_queue_contents(
        [],
        [],
        [],
        SQ1
    ).

take_0_of_queue_test() ->
    SQ = push_packets([
        ?RETAINED_PACKET_TOPIC1,
        ?RETAINED_PACKET2_TOPIC1],
        new_queue(3)),

    SQ1 = shared_queue:take(4,0,SQ),

    assert_shared_queue_contents(
        [?RETAINED_PACKET_TOPIC1],
        [?RETAINED_PACKET_TOPIC1],
        [],
        SQ1
    ).



push_packets(Packets) ->
    push_packets(Packets,new_queue(0)).

push_packets(Packets,SQ) when is_list(Packets) ->
    lists:foldl(fun shared_queue:pushr/2,SQ,Packets);

push_packets(Packets,SQ) ->
    shared_queue:pushr(Packets,SQ).


assert_shared_queue_contents(AccF,AccB,Q,SQ) ->
    AccF1 = shared_queue:get_front_acc(SQ),
    AccB1 = shared_queue:get_back_acc(SQ),
    %%Q1 = shared_queue:get_queue(SQ),
    ?assertEqual(AccF,acc_to_list(AccF1)),
    ?assertEqual(AccB,acc_to_list(AccB1)),
    ?assertEqual(Q,shared_queue:take_values(0,100,SQ)).

acc_to_list(Acc) ->
    [ V|| {_,V} <- gb_trees:to_list(Acc)].

new_queue(Seq) ->
    shared_queue:new(Seq,?MONOID,?ACCUMULATORS).
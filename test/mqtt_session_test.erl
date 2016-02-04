%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Mar 2015 12:43 AM
%%%-------------------------------------------------------------------
-module(mqtt_session_test).
-author("Kalin").

-compile(export_all).

-include("mqtt_internal_msgs.hrl").
-include_lib("eunit/include/eunit.hrl").

%% {_Topic,_Content,_Retain,_Dup,Ref},

setup_msg() ->
    Session = mqtt_session:new(),
    Ref = make_ref(),
    Packet = {<<"Topic1">>, <<"Content">>, Ref},
    [Session,Packet].

setup_sub() ->
    Session = mqtt_session:new(),
    NewSubs = [{<<"Filter1",1>>,<<"Filter2",2>>}],
    [Session,NewSubs].

msg_test_() ->
    {
        foreach, fun setup_msg/0, fun(_) -> ok end,
        [
            fun msg_in_flight_qos0/1,
            fun msg_in_flight_qos1/1,
            fun msg_in_flight_qos1_flow_complete/1,
            fun msg_in_flight_qos2_phase1/1,
            fun msg_in_flight_qos2_phase2/1,
            fun msg_in_flight_qos2_flow_complete/1
        ]
    }.

msg_in_flight_test_() ->
    {
        foreach, fun setup_msg/0, fun(_) -> ok end,
        [
            fun msg_in_flight_qos0/1,
            fun msg_in_flight_qos1/1,
            fun msg_in_flight_qos1_flow_complete/1,
            fun msg_in_flight_qos2_phase1/1,
            fun msg_in_flight_qos2_phase2/1,
            fun msg_in_flight_qos2_flow_complete/1
        ]
    }.

sub_test_() ->
    {
        foreach, fun setup_sub/0, fun(_) -> ok end,
        [
            fun subscribe_new/1,
            fun subscribe_existing/1,
            fun unsubscribe_existing/1,
            fun unsubscribe_non_existing/1
        ]
    }.

ok_on_qos0([Session,Packet]) ->
    ?_assertMatch({ok,_, undefined}, mqtt_session:append_msg(Packet,?QOS_0,Session)).

ok_on_qos1([Session,Packet]) ->
    %%?_assertMatch(({ok,_, PacketId} when is_integer(PacketId)), mqtt_session:append_msg(Session,Packet,?QOS_1)
    ?_test(
    begin
        {ok,_, PacketId} = mqtt_session:append_msg(Packet,?QOS_1,Session),
        ?assert(is_integer(PacketId))
    end
    ).

ok_on_qos2([Session,Packet]) ->
    ?_test(
        begin
            {ok,_, PacketId} = mqtt_session:append_msg(Packet,?QOS_2,Session),
            ?assert(is_integer(PacketId))
        end
    ).

qos1_flow_double_append([Session,Packet]) ->
    {ok, NewSession, _PacketId} = mqtt_session:append_msg(Packet,?QOS_1,Session),
    ?_assertMatch(duplicate, mqtt_session:append_msg(Packet,?QOS_1,NewSession)).

qo2_flow_double_append([Session,Packet]) ->
    {ok, NewSession, _PacketId} = mqtt_session:append_msg(Packet,?QOS_2,Session),
    ?_assertMatch(duplicate, mqtt_session:append_msg(Packet,?QOS_2,NewSession)).

qos1_flow_double_ack([Session,Packet]) ->
    {ok, S1, PacketId} = mqtt_session:append_msg(Packet,?QOS_1,Session),
    {ok, S2} = mqtt_session:message_ack(PacketId,S1),
    ?_assertMatch(duplicate, mqtt_session:message_ack(PacketId,S2)).

qos2_flow([S0,Packet]) ->
    ?_test(
    begin
        {ok, S1, PacketId} = mqtt_session:append_msg(Packet,?QOS_2,S0),
        {ok, S2} = mqtt_session:message_pub_rec(PacketId,S1),
        %% Test duplicate PUBREC packets
        ?assertMatch(duplicate, mqtt_session:message_pub_rec(PacketId,S2)),
        %% Test duplicate PUBCOMP packets
        {ok,S3} = mqtt_session:message_pub_comp(PacketId,S2),
        ?assertMatch(duplicate,mqtt_session:message_pub_comp(PacketId,S3))
    end).

msg_in_flight_qos0([Session,Packet]) ->
    {ok, NewSession, _PacketId} = mqtt_session:append_msg(Packet,?QOS_0,Session),
    ?_assertMatch([], mqtt_session:msg_in_flight(NewSession)).

msg_in_flight_qos1([Session,Packet]) ->
    {ok, S1, _PacketId} = mqtt_session:append_msg(Packet,?QOS_1,Session),
    ?_assertMatch([_], mqtt_session:msg_in_flight(S1)).

msg_in_flight_qos1_flow_complete([Session,Packet]) ->
    {ok, S1, PacketId} = mqtt_session:append_msg(Packet,?QOS_1,Session),
    {ok, S2} = mqtt_session:message_ack(PacketId,S1),
    ?_assertMatch([], mqtt_session:msg_in_flight(S2)).

msg_in_flight_qos2_phase1([Session,Packet]) ->
    {ok, NewSession, _PacketId} = mqtt_session:append_msg(Packet,?QOS_2,Session),
    ?_assertMatch([_], mqtt_session:msg_in_flight(NewSession)).

msg_in_flight_qos2_phase2([Session,Packet]) ->
    {ok, NewSession, PacketId} = mqtt_session:append_msg(Packet,?QOS_2,Session),
    mqtt_session:message_pub_rec(PacketId,NewSession),
    ?_assertMatch([_], mqtt_session:msg_in_flight(NewSession)).

msg_in_flight_qos2_flow_complete([Session,Packet]) ->
    {ok,S1,PacketId} = mqtt_session:append_msg(Packet,?QOS_2,Session),
    {ok,S2} = mqtt_session:message_pub_rec(PacketId,S1),
    {ok,S3} = mqtt_session:message_pub_comp(PacketId,S2),
    ?_assertMatch([], mqtt_session:msg_in_flight(S3)).


subscribe_new([Session,NewSubs]) ->
    S1 = mqtt_session:subscribe(NewSubs,Session),
    ?_assertMatch(NewSubs,mqtt_session:get_subs(S1)).

subscribe_existing([Session,NewSubs]) ->
    S1 = mqtt_session:subscribe(NewSubs,Session),
    S2 = mqtt_session:subscribe(NewSubs,S1),
    ?_assertMatch(NewSubs,mqtt_session:get_subs(S2)).

unsubscribe_existing([Session,NewSubs]) ->
    S1 = mqtt_session:subscribe(NewSubs,Session),
    Filters = [Filter || {Filter,_Qos} <- NewSubs],
    S2 = mqtt_session:unsubscribe(Filters,S1),
    ?_assertMatch([],mqtt_session:get_subs(S2)).

unsubscribe_non_existing([Session,NewSubs]) ->
    S1 = mqtt_session:subscribe(Session,NewSubs),
    S2 = mqtt_session:unsubscribe([<<"NonExisting">>],S1),
    ?_assertMatch(NewSubs,mqtt_session:get_subs(S2)).




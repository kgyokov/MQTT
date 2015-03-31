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

-include("mqtt_session.hrl").
-include_lib("eunit/include/eunit.hrl").

%% {_Topic,_Content,_Retain,_Dup,Ref},

setup() ->
    Session = mqtt_session:new(<<"Client">>,true),
    Ref = make_ref(),
    Packet = {<<"Topic1">>, <<"Content">>, false, false, Ref},
    [Session,Packet].

message_test_() ->
    {
        foreach, fun setup/0, fun(_) -> ok end,
        [
            fun proceed_on_qos0/1,
            fun proceed_on_qos1/1,
            fun proceed_on_qos2/1,
            fun deduplicate_qos1/1,
            fun deduplicate_qos2/1,
            fun qos1_flow_double_ack/1,
            fun qos2_flow/1
        ]
    }.

proceed_on_qos0([Session,Packet]) ->
    ?_assertMatch({proceed,_, undefined}, mqtt_session:append_msg(Session,Packet,?QOS_0)).

proceed_on_qos1([Session,Packet]) ->
    ?_assertMatch({proceed,_, _}, mqtt_session:append_msg(Session,Packet,?QOS_1)).

proceed_on_qos2([Session,Packet]) ->
    ?_assertMatch({proceed,_, _}, mqtt_session:append_msg(Session,Packet,?QOS_2)).

deduplicate_qos1([Session,Packet]) ->
    {proceed, NewSession, _PacketId} = mqtt_session:append_msg(Session,Packet,?QOS_1),
    ?_assertMatch(duplicate, mqtt_session:append_msg(NewSession,Packet,?QOS_1)).

deduplicate_qos2([Session,Packet]) ->
    {proceed, NewSession, _PacketId} = mqtt_session:append_msg(Session,Packet,?QOS_2),
    ?_assertMatch(duplicate, mqtt_session:append_msg(NewSession,Packet,?QOS_2)).

qos1_flow_double_ack([Session,Packet]) ->
    {proceed, S1, PacketId} = mqtt_session:append_msg(Session,Packet,?QOS_1),
    S2 = mqtt_session:message_ack(S1,PacketId),
    ?_assertMatch(S2, mqtt_session:message_ack(S2,PacketId)).

qos2_flow([S0,Packet]) ->
    ?_test(
    begin
        {proceed, S1, PacketId} = mqtt_session:append_msg(S0,Packet,?QOS_2),
        S2 = mqtt_session:message_pub_rec(S1,PacketId),
        %% Test duplicate PUBREC packets
        ?assertMatch(S2, mqtt_session:message_pub_rec(S1,PacketId)),
        %% Test duplicate PUBCOMP packets
        S3 = mqtt_session:message_pub_comp(S2,PacketId),
        ?assertMatch(S3,mqtt_session:message_pub_comp(S2,PacketId))
    end).




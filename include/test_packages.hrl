%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Mar 2017 11:55 PM
%%%-------------------------------------------------------------------
-author("Kalin").
-include("mqtt_internal_msgs.hrl").

-define(QOS_0_PACKET_TOPIC1,#packet{
    content = <<"Test">>,
    qos = ?QOS_0,
    retain = false,
    seq = 1,
    topic = <<"Topic1">>}).

-define(QOS_0_PACKET_TOPIC2,#packet{
    content = <<"Test">>,
    qos = ?QOS_0,
    retain = false,
    seq = 1,
    topic = <<"Topic2">>}).

-define(QOS_1_PACKET_TOPIC1,#packet{
    content = <<"Test">>,
    qos = ?QOS_1,
    retain = false,
    seq = 1,
    topic = <<"Topic1">>}).

-define(RETAINED_PACKET_TOPIC1,#packet{
    content = <<"Test">>,
    qos = ?QOS_0,
    retain = true,
    seq = 1,
    topic = <<"Topic1">>}).

-define(RETAINED_PACKET2_TOPIC1,#packet{
    content = <<"Test2">>,
    qos = ?QOS_0,
    retain = true,
    seq = 1,
    topic = <<"Topic1">>}).

-define(RETAINED_PACKET_TOPIC2,#packet{
    content = <<"Test">>,
    qos = ?QOS_0,
    retain = true,
    seq = 1,
    topic = <<"Topic2">>}).

-define(RETAINED_PACKET_CLEAR_TOPIC1,#packet{
    content = <<>>,
    qos = ?QOS_0,
    retain = true,
    seq = 1,
    topic = <<"Topic1">>}).


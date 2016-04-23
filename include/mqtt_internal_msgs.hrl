%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Feb 2015 5:38 PM
%%%-------------------------------------------------------------------
-author("Kalin").
-include("mqtt_packets.hrl").

-record(mqtt_message,{
	topic           ::topic(),
	content         ::binary(),
	client_id       ::client_id(),
	qos = ?QOS_0    ::qos(),
	dup = false     ::boolean(),
	retain = false  ::boolean(),
	packet_id       ::packet_id(),
    seq = 0         ::non_neg_integer()
}).

-record(msg, {
    client_id   ::client_id(),
    client_seq  ::non_neg_integer(),
    timestamp   ::term(),
    topic       ::topic(),
    content     ::binary(),
    qos         ::qos()
}).

-record(packet,{
    topic       ::topic(),
    content     ::content(),
    retain      ::boolean(),
    qos         ::qos(),
    seq ::any()
}).


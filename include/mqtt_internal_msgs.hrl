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

-type subscription() :: {Topic::binary(),QoS::qos()}.

-record(mqtt_message,{
	topic           ::binary(),
	content         ::binary(),
	client_id       ::binary(),
	qos = ?QOS_0    ::qos(),
	dup = false     ::boolean(),
	retain = false  ::boolean(),
	packet_id       ::packet_id(),
    seq = 0         ::non_neg_integer()
}).

-record(msg, {
    client_id,
    client_seq,
    timestamp,
    topic,
    content,
    qos
}).


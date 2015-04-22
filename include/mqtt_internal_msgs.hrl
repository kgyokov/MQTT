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
	topic,
	content,
	client_id,
	qos = 0,
	dup = false,
	retain = false,
	packet_id = undefined,
    seq = 0
}).

-record(msg, {
    client_id,
    client_seq,
    timestamp,
    topic,
    content,
    qos
}).


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

-type subscription() :: {Topic::binary(),QoS::byte()}.

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

-record(session_in,{
	client_id                 ::binary(),
    is_persistent  = false    ::boolean(),         %% whether the session needs to be persisted
	packet_seq = 0            ::non_neg_integer(), %% packet sequence number (ever increasing)
	msg_in_flight             ::#mqtt_message{},
	qos2_rec = gb_sets:new()  ::gb_sets:set(),
	will                      ::#will_details{}
}).
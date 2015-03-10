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
	packet_id = undefined
}).

-record(session_out,{
	client_id                 ::binary(),            %% The id of the client
    is_persistent             ::boolean(),           %% whether the session needs to be persisted
	packet_seq                ::non_neg_integer(),   %% The latest packet id (incremented by 1 for every packet
	qos1 = dict:new()         ,
	qos2 = dict:new()         ,
	qos2_rec = gb_sets:new()  ,
	refs = gb_sets:new()      ,
	subscriptions = []        ::[subscription()]
}).

-record(session_in,{
	client_id                 ::binary(),
	packet_seq                ::non_neg_integer(), %% packet sequence number (ever increasing)
	msg_in_flight             ::#mqtt_message{},
	qos2_rec = dict:new(),
	will                      ::#will_details{}
}).
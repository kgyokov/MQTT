%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Feb 2015 5:37 PM
%%%-------------------------------------------------------------------
-module(mqtt_session).
-author("Kalin").

-include("mqtt_packets.hrl").
-include("mqtt_session.hrl").
%% API
-export([append_msg/4,
	append_message_comp/2,
	message_ack/2,
	message_pub_rec/2,
	message_pub_comp/2,
	recover/1, subscribe/2]).


subscribe(Session = #session_out{subscriptions = Subs},NewSubs) ->

	Results = [ {add_or_replace(NewSub,Subs),NewSub} || NewSub <- NewSubs ],
	ok.

unsubscribe(Session = #session_out{subscriptions = Subs},Subs) ->
	ok.


add_or_replace({Topic,QoS},Subs)->
	State = case dict:find(Topic,Subs) of
		        error ->
			        new;
		        {ok,CurrentQoS} when CurrentQoS =/= QoS ->
			        replaced;
		        {ok,CurrentQoS} when CurrentQoS =:= QoS ->
			        exists
	        end,
	dict:store(Topic,QoS,Subs),
	State.

%%
%% @doc
%% Appends message
%%
%%
%%
%% @end
append_msg(Session,
    CTRPacket = {_Content,Topic,_Retain},
    OriginalQoS,
    Ref) ->
	#session_out{packet_seq = PacketSeq,
		refs = Refs,
		subscriptions = Subs} = Session,

	case gb_sets:is_member(Ref,Refs) of
		false ->
			MatchingSub = dict:find(Topic,Subs),
			case MatchingSub of
				error ->
					ok;
				SubQoS ->
					QoS = min(OriginalQoS,SubQoS),
					forward_msg(Session,CTRPacket,QoS,Ref)
			end;
		true ->
			duplicate
	end.

forward_msg(Session,CTRPacket,QoS,Ref)->
	#session_out{packet_seq = PacketSeq, refs = Refs} = Session,

	PacketId = if QoS =:= ?QOS_AT_LEAST_ONCE;
	QoS =:= ?QOS_EXACTLY_ONCE ->
		(PacketSeq+1) band 16#ffff;
		           true ->
			           undefined
	           end,
	Session1 = Session#session_out{refs = gb_sets:add(Ref,Refs)},
	NewSession = (case QoS of
		              ?QOS_AT_LEAST_ONCE ->
			              #session_out{qos1 = QosQueue} = Session1,
			              Session1#session_out{qos1 = dict:store(PacketId ,CTRPacket,QosQueue),
				              packet_seq = PacketId};
		              ?QOS_EXACTLY_ONCE ->
			              #session_out{qos2 = QosQueue} = Session1,
			              Session1#session_out{qos2 = dict:store(PacketId ,CTRPacket,QosQueue),
				              packet_seq = PacketId};
		              ?QOS_AT_MOST_ONCE ->
			              Session
	              end),
%%       #session_out{refs = gb_sets:add(Ref,Refs)},
	{ok, NewSession,to_publish(QoS,{PacketId,CTRPacket},false)}
.

append_message_comp(Session = #session_out{refs = Refs}, Ref) ->
	Session#session_out{refs = gb_sets:delete(Ref,Refs)}.

message_ack(Session,PacketId) ->
	#session_out{qos1 = Messages} = Session,
	Session#session_out{qos1 = dict:erase(PacketId,Messages)}.


message_pub_rec(Session,PacketId) ->
	#session_out{qos2 = Msgs, qos2_rec = Ack} = Session,
	case dict:find(PacketId,Msgs) of
		{ok,_} ->
			Session#session_out{qos2 = dict:erase(PacketId,Msgs),
				qos2_rec = gb_sets:add(PacketId,Ack)};
		error ->
			Session
	end.


message_pub_comp(Session = #session_out{},PacketId)  ->
	#session_out{qos2_rec = Ack} = Session,
	Session#session_out{qos2_rec = gb_sets:delete(PacketId,Ack)}.

recover(Session) ->
	recover_in_flight(Session),
	get_retained(Session),
	recover_queued(Session)
.

recover_in_flight(#session_out{qos1 = UnAck1, qos2 = UnAck2,
	qos2_rec = Rec, packet_seq = PacketSeq}) ->
	NewPackets =
		[ to_publish(?QOS_AT_LEAST_ONCE,Packet,true) || Packet  <- dict:to_list(UnAck1)] ++
		[ to_publish(?QOS_AT_MOST_ONCE,Packet,true)  || Packet  <- dict:to_list(UnAck2)] ++
		[ to_pubrel(PacketId) || {PacketId,PacketId}  <- gb_sets:to_list(Rec)],
	{PacketSeq,NewPackets}.

recover_queued(Session)->
	ok.

get_retained(Session) ->
	ok.

%% to_publish(QoS,Packet) ->
%%   to_publish(QoS,Packet,true).

to_publish(QoS,{PacketId,{Content,Topic,Retain}},Dup) ->
	#'PUBLISH'{content = Content,packet_id = PacketId,
		qos = QoS,topic = Topic,
		dup = Dup,retain = Retain}.

to_pubrel(PacketId) ->
	#'PUBREL'{packet_id = PacketId}.


new(ClientId)->
	#session_out{
		client_id = ClientId,
		packet_seq = 0,
		qos1 = dict:new(),
		qos2 = dict:new(),
		qos2_rec = gb_sets:new(),
		refs = gb_sets:new(),
		subscriptions = []
	}.
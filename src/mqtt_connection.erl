%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2014 6:14 PM
%%%-------------------------------------------------------------------
-module(mqtt_connection).
-author("Kalin").

-include("mqtt_packets.hrl").
-include("mqtt_session.hrl").
-behaviour(gen_server).

%% API
-export([start_link/2,
	process_packet/2,
	process_bad_packet/2,
	process_unexpected_disconnect/2,
	close_duplicate/1,
	publish_packet/2]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-define(SERVER, ?MODULE).
-define(CONNECT_DEFAULT_TIMEOUT, 30000).



-record(state, {
	client_id,
	connect_state = connecting, %% CONNECT state: connecting, connected, disconnecting, disconnected
	sender_pid,                 %% The process sending to the actual device
	receiver_pid,               %% The process receiving from the actual device TODO: Do we even need to know this???
	options,                    %% options such as connection timeouts, etc.
	session = #{},
	keep_alive_ref = undefined, %% so we can ignore old keep-alive timeout messages after restarting the timer
	keep_alive_timeout = undefined,
	will,
	security,
	auth_ctx                    %% Authorization/Authentication context
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(SenderPid::pid(),Options::term()) ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(SenderPid,Options) ->
	gen_server:start_link(?MODULE, [SenderPid,Options], []).

publish_packet(Pid,Packet) ->
	gen_server:cast(Pid,{publish,Packet}).

process_packet(Pid,Packet) ->
	gen_server:cast(Pid,{packet, Packet}).

process_bad_packet(Pid,Reason) ->
	gen_server:cast(Pid,{malformed_packet,Reason}).

process_unexpected_disconnect(Pid,Reason) ->
	gen_server:cast(Pid,{client_disconnected, Reason}).

close_duplicate(Pid) ->
	gen_server:cast(Pid, {force_close, duplicate}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
%%  -spec(init(Args :: term()) ->
%%    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
%%    {stop, Reason :: term()} | ignore).
init([SenderPid,Options]) ->
	process_flag(trap_exit,true),

	ConnectTimeOut = proplists:get_value(connect_timeout,Options,?CONNECT_DEFAULT_TIMEOUT),
	set_connect_timer(ConnectTimeOut),
	{Security,SecConf} = proplists:get_value(security,{gen_auth_default,undefined}),

	{ok, #state{
		connect_state = starting,
		sender_pid =  SenderPid,
		receiver_pid = undefined, %% TODO decide how to handle this
		options = Options,
		security = {Security,SecConf}
	}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
	State :: #state{}) ->
	{reply, Reply :: term(), NewState :: #state{}} |
	{reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
	{stop, Reason :: term(), NewState :: #state{}}).


handle_call({publish, {Message,Topic,QoS,PacketId,Retain}}, _From, S)->
	send_to_client(S,#'PUBLISH'{
		content = Message,
		topic = Topic,
		qos = QoS,
		dup = error(not_implemented),
		packet_id = PacketId,
		retain = Retain
	});

handle_call({malformed_packet,_Reason}, _From, S) ->
	abort_connection(S, malformed_packet);

handle_call({client_disconnected, _Reason}, _From, S) ->
	{stop,normal, S#state{connect_state = disconnecting}};

handle_call({force_close, Reason}, _From, S) ->
	abort_connection(S,Reason);


handle_call(_Request, _From, State) ->
	{reply, ok, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), NewState :: #state{}}).

handle_cast({packet, Packet}, S) ->
	S1 = reset_keep_alive(S),
	handle_packet(Packet, S1);

handle_cast({force_close,_Reason}, State) ->
	{stop, normal, State#state{connect_state = disconnecting}};

handle_cast(_Request, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
	{noreply, NewState :: #state{}} |
	{noreply, NewState :: #state{}, timeout() | hibernate} |
	{stop, Reason :: term(), NewState :: #state{}}).


handle_info(connect_timeout, S = #state{connect_state = connecting})->
	disconnect_client(S,connect_timeout),
	{stop, normal, S#state{connect_state = disconnecting}};

handle_info({keep_alive_timeout,Ref}, S = #state{ keep_alive_ref = Ref})->
	disconnect_client(S,keep_alive_timeout),
	{stop, normal, S#state{connect_state = disconnecting}};

handle_info(_Info, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
	State :: #state{}) -> term()).
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
	Extra :: term()) ->
	{ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%------------------------------------------------
%% CONNECT handling
%%------------------------------------------------

%% Only accept CONNECT-s when we are NOT connected yet, otherwise disconnect with error
handle_packet(#'CONNECT'{}, S = #state{connect_state = ConnectState})
	when ConnectState =/= connecting ->
	abort_connection(S, duplicate_CONNECT);

%% Only accept MQTT
handle_packet(#'CONNECT'{protocol_name = ProtocolName},
	S = #state{connect_state = connecting}
) when ProtocolName =/= <<"MQTT">> ->
	abort_connection(S, unknown_protocol);

%% Only accept version 4
handle_packet(#'CONNECT'{protocol_version = ProtocolVersion},
	S = #state{connect_state = connecting}
) when ProtocolVersion =/= 4 ->
	send_to_client(S,#'CONNACK'{return_code = ?UNACCEPTABLE_PROTOCOL}),
	abort_connection(S, unknown_protocol_version);

handle_packet(#'CONNECT'{client_id = <<>>,clean_session = false}, S) ->
	send_to_client(S, #'CONNACK'{return_code = ?IDENTIFIER_REJECTED}),
	disconnect_client(S,invalid_client_id);


%% Valid packet w/o Client Id
handle_packet(Packet = #'CONNECT'{client_id = <<>>,clean_session = true}, S) ->
	ClientId = auto_generate_client_id(),
	handle_packet(Packet#'CONNECT'{client_id = ClientId}, S);


%% Valid complete packet!
handle_packet(#'CONNECT'{ client_id = ClientId,keep_alive = KeepAliveTimeout,
	clean_session = CleanSession,will = Will,
	password = Password,username = Username},
	%%disallow duplicate CONNECT packets
	S = #state{connect_state = connecting,security = {Security,SecConf}}) ->

	%%
	%%
	%% TODO: validate connect packet
	%%
	%% @todo: authorize Will?!?!?
	%%

	case Security:authenticate(SecConf,ClientId,Username,Password) of
		{error,Reason} ->
			Code = case Reason of
				       bad_credentials ->
					       ?BAD_USERNAME_OR_PASSWORD;
				       _ ->
					       ?UNAUTHORIZED
			       end,
			send_to_client(S,#'CONNACK'{session_present = 0,return_code = Code}),
			disconnect_client(S,bad_auth);
		{ok, AuthCtx} ->
			S1 = S#state{auth_ctx = AuthCtx},
			register_self(ClientId),

			SessionPresent = if(CleanSession) ->
				mqtt_session_repo:clear(ClientId);
				                 false,
				                 true ->
					                 true  %% @todo: determine session state
			                 end,


			S2 = start_keep_alive(S1, KeepAliveTimeout),

			%% @todo:  Determine session present
			send_to_client(S, #'CONNACK'{return_code = ?CONECTION_ACCEPTED, session_present = SessionPresent}),
			S3 = S2#state{client_id = ClientId,connect_state = connected,will = Will},
			{noreply,S3}
	end;

%% Catch- all case
handle_packet(Packet, S = #state{ connect_state = connecting})
	when not is_record(Packet, 'CONNECT') ->
	abort_connection(S, 'CONNECT_expected');


handle_packet(Packet = #'PUBLISH'{topic = Topic},
	S = #state{security = {Security,_},auth_ctx = AuthCtx}) ->
	case Security:authorize(AuthCtx,publish,Topic) of
		ok ->
			handle_publish(Packet,S),
			{noreply,S};
		{error,_Details}->
			abort_connection(S,unauthorized)
	end;


handle_packet(#'PUBACK'{packet_id = PacketId}, S = #state{client_id = ClientId}) ->
	mqtt_session:message_ack(ClientId,PacketId),
	{noreply,S};

handle_packet(#'PUBREC'{packet_id = PacketId}, S = #state{client_id = ClientId}) ->
	mqtt_session:message_pub_rec(ClientId,PacketId),
	{noreply,S};

handle_packet(#'PUBCOMP'{packet_id = PacketId}, S = #state{client_id = ClientId}) ->
	mqtt_session:message_pub_comp(ClientId,PacketId),
	{noreply,S};

handle_packet(#'PUBREL'{packet_id = PacketId}, S) ->
	mqtt_publish:exactly_once_phase2(PacketId, S),
	send_to_client(S,#'PUBCOMP'{packet_id = PacketId}),
	{noreply,S};

handle_packet(#'SUBSCRIBE'{subscriptions = []}, S) ->
	abort_connection(S,protocol_violation);

handle_packet(#'SUBSCRIBE'{packet_id = PacketId,subscriptions = Subs},
			  S = #state{client_id = ClientId,session = CleanSession,
			             security = {Security,_},auth_ctx = AuthCtx }) ->

	%% TODO: Use CleanSession to determine what to do


	ok = if CleanSession ->
		mqtt_session_repo:clear(ClientId);
		     true -> ok
	     end,



	Results = [
		case Security:authorize(AuthCtx,subscribe,Sub) of
			ok ->
				case mqtt_session:subscribe(ClientId,Sub) of
					{error,_}->
						?SUBSCRIPTION_FAILURE;
					{ok,QoS}->
						QoS
				end;
			{error,_}->
				?SUBSCRIPTION_FAILURE
		end
		|| Sub  <- Subs],
	Ack = #'SUBACK'{packet_id = PacketId,return_codes = Results},
	send_to_client(S,Ack),
	{noreply,S};


handle_packet(#'UNSUBSCRIBE'{packet_id = PacketId,topic_filters = Filters},
	S = #state{client_id = ClientId}) ->
	[ ok = mqtt_session:unsubscribe(ClientId,Filter) || Filter <- Filters],
	Ack = #'UNSUBACK'{packet_id = PacketId},
	send_to_client(S,Ack),
	{noreply,S};


handle_packet(#'PINGREQ'{}, S) ->
	send_to_client(S, #'PINGRESP'{}),
	{noreply,S};


handle_packet(#'DISCONNECT'{}, S) ->
	%% Graceful disonnect. We must NOT publish a Will message
	disconnect_client(S, client_request),
	{stop,normal,
		{disconnected, normal},
		S#state{connect_state = disconnecting}};

handle_packet(_, S) ->
	abort_connection(S,malformed_packet).


%% =================================================
%% Publish
%% =================================================

handle_publish(#'PUBLISH'{packet_id = PacketId,retain = Retain,
	qos = Qos,content = Content,
	dup = Dup,topic = Topic},
	S = #state{client_id = ClientId}) ->
	%% Map packet ot internal representation
	Msg = #mqtt_message{packet_id = PacketId,client_id = ClientId,
		content = Content,dup = Dup,
		qos = Qos,retain = Retain,
		topic = Topic},
	publish(Msg,S).

publish(Msg = #mqtt_message{packet_id = PacketId, qos = Qos},S)->
	case Qos of
		0 ->
			mqtt_publish:at_most_once(Msg,S);
		1 ->
			mqtt_publish:at_least_once(Msg,S),
			send_to_client(S, #'PUBACK'{packet_id = PacketId});
		2 ->
			mqtt_publish:exactly_once_phase1(Msg,S),
			send_to_client(S, #'PUBREC'{packet_id = PacketId})
	end.


%%%===================================================================
%% SESSION interaction
%%%===================================================================

register_self(ClientId) ->
	mqtt_registration_repo:register(self(),ClientId).

unregister_self(ClientId) ->
	mqtt_registration_repo:unregister(self(),ClientId).

%% =================================================
%% Timer
%% =================================================

set_connect_timer(Timeout)->
	timer:send_after(Timeout, connect_timeout).

start_keep_alive(S,TimeOut)->
	Ref = set_keep_alive_timer(TimeOut),
	S#state {keep_alive_ref = Ref,keep_alive_timeout = TimeOut}.

set_keep_alive_timer(Timeout)->
	timer:send_after(Timeout, keep_alive_timeout).

reset_keep_alive(S = #state{keep_alive_ref = Ref,keep_alive_timeout = TimeOut}) ->

	case TimeOut of
		undefined
			-> S;
		_
			-> S#state{keep_alive_ref = reset_timer(TimeOut,Ref)}
	end
.

reset_timer(Ref,TimeOut)->
	if Ref =/= undefned ->
		timer:cancel(Ref);
		true -> ok
	end,
	set_keep_alive_timer(TimeOut).

%% =================================================
%% Communication with Sender process
%% =================================================

send_to_client(#state{sender_pid = SenderPid},Packet)->
	mqtt_sender:send_packet(SenderPid,Packet);

send_to_client(SenderPid,Packet) when is_pid(SenderPid)->
	mqtt_sender:send_packet(SenderPid,Packet).


abort_connection(S = #state{will = Will, client_id = ClientId},Reason) ->
	case Will of
		undefined ->
			ok;
		#will_details{message = Content, topic = Topic,
			qos = QoS, retain = Retain} ->
			publish(#mqtt_message{topic = Topic, retain = Retain,
				qos = QoS, client_id = ClientId,
				content = Content}, S)
	end,
	disconnect_client(S,Reason)
.

disconnect_client(S,Reason) ->
	%% gen_server:call(ClientPid,{disconnect,Reason})
	{stop,normal,
		S#state{connect_state = disconnecting}}.


%% ==========================================================
%% Misc.
%% ==========================================================

auto_generate_client_id() ->
	base64:encode_to_string("__" ++ crypto:rand_bytes(24)).

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

%%-include("mqtt_packets.hrl").
-include("mqtt_session.hrl").
-behaviour(gen_server).

%% API
-export([start_link/3,
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
    clean_session,
    session_in,
    session_out,
    keep_alive_ref = undefined, %% so we can ignore old keep-alive timeout messages after restarting the timer
    keep_alive_timeout = undefined,
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
-spec(start_link(ReceiverPid::pid,SenderPid::pid(),Options::term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(ReceiverPid,SenderPid,Options) ->
    gen_server:start_link(?MODULE, [ReceiverPid,SenderPid,Options], []).

publish_packet(Pid,Packet) ->
    gen_server:cast(Pid,{publish,Packet}).

process_packet(Pid,Packet) ->
    gen_server:cast(Pid,{packet, Packet}).

process_bad_packet(Pid,Reason) ->
    gen_server:cast(Pid,{malformed_packet,Reason}).

process_unexpected_disconnect(Pid,Reason) ->
    gen_server:cast(Pid,{client_disconnected, Reason}).


%%--------------------------------------------------------------------
%% @doc
%% Closes the connection if we detect two connections from the same ClientId
%% @end
%%--------------------------------------------------------------------
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
init([ReceiverPid,SenderPid,Options]) ->
    process_flag(trap_exit,true),
    {Security,SecConf} = proplists:get_value(security,Options,{gen_auth_default,undefined}),
    ConnectTimeOut = proplists:get_value(connect_timeout,Options,?CONNECT_DEFAULT_TIMEOUT),

    set_connect_timer(ConnectTimeOut),

    self() ! async_init,

    {ok, #state{
        connect_state = connecting,
        sender_pid =  SenderPid,
        receiver_pid = ReceiverPid, %% TODO decide how to handle this
        options = Options,
        security = {Security,SecConf}
    }}
.

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

handle_cast({publish, {Message,Topic,QoS,PacketId,Retain}}, S)->
    send_to_client(S,#'PUBLISH'{
        content = Message,
        topic = Topic,
        qos = QoS,
        dup = error(not_implemented),
        packet_id = PacketId,
        retain = Retain
    });

handle_cast({malformed_packet,_Reason}, S) ->
    abort_connection(S, malformed_packet);

handle_cast({client_disconnected, _Reason}, S) ->
    receiver_closing(S, client_disconnected);

handle_cast({force_close, Reason}, S = #state{connect_state = connected}) ->
    abort_connection(S,Reason);

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

handle_info(async_init, S = #state{receiver_pid = ReceiverPid}) ->
    link(ReceiverPid),
    {noreply,S};

handle_info({'EXIT',ReceiverPid,Reason}, S = #state{receiver_pid = ReceiverPid,
                                                    connect_state = connected}) ->
    receiver_closing(S,Reason);

handle_info(connect_timeout, S = #state{connect_state = connecting}) ->
    prevent_connection(S,timeout);

handle_info({keep_alive_timeout,Ref}, S = #state{ keep_alive_ref = Ref}) ->
    abort_connection(S,timeout);

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

terminate(normal, _State) ->
    %% This is an expected termination, nothing left to do
    ok;

terminate(shutdown, S = #state{connect_state = connected}) ->
    %% We are connected, this is an unexpected termination!!!
    bad_disconnect(S);

terminate(_OtherError, S = #state{connect_state = connected}) ->
    %% We are connected, this is an unexpected shutdown!!!
    %% @todo: maybe try to disconnect the client???
    bad_disconnect(S);

%%
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
handle_packet(#'CONNECT'{protocol_name = ProtocolName}, S)
    when ProtocolName =/= <<"MQTT">> ->
    prevent_connection(S, unknown_protocol);

%% Only accept version 4
handle_packet(#'CONNECT'{protocol_version = ProtocolVersion}, S)
    when ProtocolVersion =/= 4 ->
    send_to_client(S,#'CONNACK'{return_code = ?UNACCEPTABLE_PROTOCOL}),
    prevent_connection(S, unknown_protocol_version);

handle_packet(#'CONNECT'{client_id = <<>>,clean_session = false}, S) ->
    send_to_client(S, #'CONNACK'{return_code = ?IDENTIFIER_REJECTED}),
    prevent_connection(S,invalid_client_id);


%% Valid packet w/o Client Id
handle_packet(Packet = #'CONNECT'{client_id = <<>>,clean_session = true}, S) ->
    ClientId = auto_generate_client_id(),
    handle_packet(Packet#'CONNECT'{client_id = ClientId}, S);


%% Valid complete packet!
handle_packet(#'CONNECT'{client_id = ClientId,keep_alive = KeepAliveTimeout,
                         clean_session = CleanSession,will = Will,
                         password = Password,username = Username},
              S = #state{connect_state = connecting,
                         security = {Security,SecConf}}) ->

    %%=======================================================================
    %% @todo: validate connect packet
    %%=======================================================================

    %%=======================================================================
    %% @todo: authorize Will?!?!?
    %%=======================================================================

    case Security:authenticate(SecConf,ClientId,Username,Password) of
        {error,Reason} ->
            Code = case Reason of
                       bad_credentials ->
                           ?BAD_USERNAME_OR_PASSWORD;
                       _ ->
                           ?UNAUTHORIZED
                   end,
            send_to_client(S,#'CONNACK'{session_present = false,return_code = Code}),
            prevent_connection(S,bad_auth);
        {ok, AuthCtx} ->
            S1 = S#state{auth_ctx = AuthCtx},
            %%=======================================================================
            %% @todo: determine session state
            %%=======================================================================
%% 			SessionPresent = if(CleanSession) ->
%% 				mqtt_session_repo:clear(ClientId);
%% 				                 false,
%% 				                 true ->
%% 					                 true  %% @todo: determine session state
%% 			                 end,

            register_self(ClientId,CleanSession),
            SessionPresent = case CleanSession of
                                 false -> error({not_supported,persistent_session});
                                 true -> false
                             end,

            S3 = (start_keep_alive(S1, KeepAliveTimeout * 1000))
            #state{session_in = #session_in{client_id = ClientId,will = Will},
            session_out = #session_out{client_id = ClientId,is_persistent = CleanSession}},

            %% @todo:  Determine session present
            send_to_client(S, #'CONNACK'{return_code = ?CONECTION_ACCEPTED,
                                         session_present = SessionPresent}),
            S4 = S3#state{client_id = ClientId,connect_state = connected},
            {noreply,S4}
    end;

%% Catch- all case
handle_packet(Packet, S = #state{ connect_state = connecting})
    when not is_record(Packet, 'CONNECT') ->
    prevent_connection(S, 'CONNECT_expected');


handle_packet(Packet = #'PUBLISH'{topic = Topic},
    S = #state{security = {Security,_},auth_ctx = AuthCtx}) ->
    case Security:authorize(AuthCtx,publish,Topic) of
        ok ->
            S1 = handle_publish(Packet,S),
            {noreply,S1};
        {error,_Details} ->
            abort_connection(S,unauthorized)
    end;


handle_packet(#'PUBACK'{packet_id = PacketId}, S = #state{session_out = SessionOut}) ->
    mqtt_session:message_ack(SessionOut,PacketId),
    {noreply,S};

handle_packet(#'PUBREC'{packet_id = PacketId}, S = #state{session_out = SessionOut}) ->
    mqtt_session:message_pub_rec(SessionOut,PacketId),
    {noreply,S};

handle_packet(#'PUBCOMP'{packet_id = PacketId}, S = #state{session_out = SessionOut}) ->
    mqtt_session:message_pub_comp(SessionOut,PacketId),
    {noreply,S};

handle_packet(#'PUBREL'{packet_id = PacketId}, S = #state{session_in = SessionIn}) ->
    S1 = S#state{session_in = mqtt_publish:exactly_once_phase2(PacketId, SessionIn)},
    send_to_client(S1,#'PUBCOMP'{packet_id = PacketId}),
    {noreply,S1};

handle_packet(#'SUBSCRIBE'{subscriptions = []}, S) ->
    abort_connection(S,protocol_violation);

handle_packet(#'SUBSCRIBE'{packet_id = PacketId,subscriptions = Subs},
                S = #state{client_id = ClientId,security = {Security,_},
                           session_in = SessionIn, auth_ctx = AuthCtx }) ->

    %%=======================================================================
    %% TODO: Use CleanSession to determine what to do
    %%=======================================================================

    Results = [
        case Security:authorize(AuthCtx,subscribe,Sub) of
            ok ->
                case mqtt_session:subscribe(SessionIn,Sub) of
                    {error,_} ->
                        ?SUBSCRIPTION_FAILURE;
                    {ok,QoS} ->
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
    S = #state{session_in = SessionIn}) ->
    [ ok = mqtt_session:unsubscribe(SessionIn,Filter) || Filter <- Filters],
    Ack = #'UNSUBACK'{packet_id = PacketId},
    send_to_client(S,Ack),
    {noreply,S};


handle_packet(#'PINGREQ'{}, S) ->
    send_to_client(S, #'PINGRESP'{}),
    {noreply,S};


handle_packet(#'DISCONNECT'{}, S) ->
    %% Graceful disonnect. We must NOT publish a Will message
    graceful_disconnect(S);

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
    S#state{session_in = publish(Msg,S)}.

publish(Msg = #mqtt_message{packet_id = PacketId, qos = Qos},
        S = #state{session_in = SessionIn, sender_pid = SenderPid})->
    NewSessionIn = case Qos of
        0 ->
            mqtt_publish:at_most_once(Msg,SessionIn);
        1 ->
            Sess1 = mqtt_publish:at_least_once(Msg,SessionIn),
            send_to_client(SenderPid, #'PUBACK'{packet_id = PacketId}),
            Sess1;
        2 ->
            Sess2 = mqtt_publish:exactly_once_phase1(Msg,SessionIn),
            send_to_client(SenderPid, #'PUBREC'{packet_id = PacketId}),
            Sess2
    end,
    NewSessionIn
.


%%%===================================================================
%% SESSION interaction
%%%===================================================================

register_self(_ClientId,_CleanSession) ->
    %%mqtt_registration_repo:register(self(),ClientId)
    ok.

unregister_self(_ClientId,_CleanSession) ->
    %%mqtt_registration_repo:unregister(self(),ClientId).
    ok.

%% =================================================
%% Timer
%% =================================================

set_connect_timer(Timeout) ->
    timer:send_after(Timeout, connect_timeout).

start_keep_alive(S,TimeOut) ->
    Ref = set_keep_alive_timer(TimeOut),
    S#state {keep_alive_ref = Ref,keep_alive_timeout = TimeOut}.

set_keep_alive_timer(Timeout) ->
    timer:send_after(Timeout, keep_alive_timeout).

reset_keep_alive(S = #state{keep_alive_ref = Ref,keep_alive_timeout = TimeOut}) ->

    case TimeOut of
        undefined
            -> S;
        _
            -> S#state{keep_alive_ref = reset_timer(TimeOut,Ref)}
    end
.

reset_timer(Ref,TimeOut) ->
    if Ref =/= undefned ->
        timer:cancel(Ref);
        true -> ok
    end,
    set_keep_alive_timer(TimeOut).

%% =================================================
%% Communication with Sender process
%% =================================================

send_to_client(#state{sender_pid = SenderPid},Packet) ->
    mqtt_sender:send_packet(SenderPid,Packet);

send_to_client(SenderPid,Packet) when is_pid(SenderPid) ->
    mqtt_sender:send_packet(SenderPid,Packet).

%% =================================================
%% Handling disconnect and clean-up
%% =================================================


%% Receiver initiated,  connecting  -> receiver_closing
%% Receiver initiated,  connected   -> receiver_closing
%% Server initiated,    connecting  -> prevent_connection
%% Server initiated,    connected   -> abort_connection
%% Graceful,            connected   -> graceful_disconnect
%% _                                -> let it fail


%% @doc
%% The receiver terminates an attempt to establish a connection.
%% (e.g. due to a network error)
%% @end
receiver_closing(S = #state{connect_state = connecting},Reason) ->
    %% We are not even connected, nothing to do here
    {stop, normal,
        S#state{connect_state = {closing,receiver,Reason}}};

%% @doc
%% The receiver terminates an established connection.
%% (e.g. due to a network error)
%% @end
receiver_closing(S = #state{connect_state = connected},Reason) ->
    %% We need to clean up
    bad_disconnect(S),
    {stop, normal,
        S#state{connect_state = {closing,receiver,Reason}}}.

%% @doc
%% The server terminates an attempt to establish a connection.
%% (e.g. due to a bad authorization, etc.)
%% @end
abort_connection(S = #state{connect_state = connecting},Reason) ->
    prevent_connection(S,Reason);

%% @doc
%% The server terminates an existing connection.
%% (e.g. due to a bad request)
%% @end
abort_connection(S = #state{connect_state = connected},Reason) ->
    bad_disconnect(S),
    disconnect_client(S,Reason),
    {stop, normal,
        S#state{connect_state = {closing,server,Reason}}}.

%% @doc
%% The server terminates an attempt to establish a connection.
%% (e.g. due to a bad authorization, etc.)
%% @end
prevent_connection(S,Reason) ->
    disconnect_client(S,Reason),
    {stop, normal,
        S#state{connect_state = {closing,receiver,Reason}}}.

%% @doc
%% The client tells the server it wants to close the established connection.
%% The server reacts accordingly.
%% @end
graceful_disconnect(S = #state{session_in = SessionIn}) ->
    error_logger:info_msg("SessionIn is ~p",[SessionIn]),
    S1 = S#state{session_in = mqtt_publish:discard_will(SessionIn)},
    session_cleanup(S1),
    disconnect_client(S1, graceful),
    {stop,normal,
        S1#state{connect_state = {closing,graceful,normal}}}.

bad_disconnect(S) ->
    session_cleanup(S).

session_cleanup(#state{client_id = ClientId, clean_session = CleanSession}) ->
    unregister_self(ClientId,CleanSession).

disconnect_client(_S,_Reason) ->
    ok. %% @todo: Do we even need this?


%% ==========================================================
%% Misc.
%% ==========================================================

auto_generate_client_id() ->
    base64:encode_to_string("__" ++ crypto:rand_bytes(24)).

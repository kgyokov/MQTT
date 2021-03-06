%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%% gen_server to manage a connection
%%% @end
%%% Created : 06. Dec 2014 6:14 PM
%%%-------------------------------------------------------------------
-module(mqtt_connection).
-author("Kalin").

%%-include("mqtt_packets.hrl").
-include("mqtt_internal_msgs.hrl").
-behaviour(gen_server).

%% API
-export([start_link/4,
         process_packet/2,
         process_bad_packet/2,
         process_unexpected_disconnect/2,
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
    sup_pid,
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
-spec(start_link(ReceiverPid::pid,SenderPid::pid(),SupPid::pid(),Options::term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(ReceiverPid,SenderPid,SupPid,Options) ->
    gen_server:start_link(?MODULE, [ReceiverPid,SenderPid,SupPid,Options], []).

publish_packet(Pid,Packet) ->
    gen_server:cast(Pid,{publish,Packet}).

process_packet(Pid,Packet) ->
    gen_server:cast(Pid,{packet, Packet}).

process_bad_packet(Pid,Reason) ->
    gen_server:cast(Pid,{malformed_packet,Reason}).

process_unexpected_disconnect(Pid,Reason) ->
    gen_server:cast(Pid,{client_disconnected, Reason}).


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
init([ReceiverPid,SenderPid,SupPid,Options]) ->
    process_flag(trap_exit,true),
    link(ReceiverPid),
    {Security,SecConf} = proplists:get_value(security,Options,{gen_auth_default,undefined}),
    ConnectTimeOut = proplists:get_value(connect_timeout,Options,?CONNECT_DEFAULT_TIMEOUT),
    set_connect_timer(ConnectTimeOut),
    {ok, #state{
        connect_state = connecting,
        sender_pid =  SenderPid,
        receiver_pid = ReceiverPid, %% TODO decide how to handle this
        sup_pid = SupPid,
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
    S1 = maybe_reset_keep_alive(S),
    handle_packet(Packet, S1);

handle_cast({publish, Packet}, S = #state{sender_pid = SenderPid}) ->
    send_to_client(SenderPid,Packet),
    {noreply,S};

handle_cast({malformed_packet,_Reason}, S) ->
    abort_connection(S, malformed_packet);

handle_cast({client_disconnected, _Reason}, S) ->
    receiver_closing(S, client_disconnected);

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

handle_info({'EXIT',ReceiverPid,Reason}, S = #state{receiver_pid = ReceiverPid,
                                                    connect_state = connected}) ->
    receiver_closing(S,Reason);

handle_info(connect_timeout, S = #state{connect_state = connecting}) ->
    prevent_connection(S,timeout);

handle_info({keep_alive_timeout,MatchRef}, S = #state{ keep_alive_ref = {_,MatchRef}}) ->
    error_logger:info_msg("Connection timed out ~p",[MatchRef]),
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
    send_to_client(S,#'CONNACK'{return_code = ?CONNACK_UNACCEPTABLE_PROTOCOL}),
    prevent_connection(S, unknown_protocol_version);

handle_packet(#'CONNECT'{client_id = <<>>,clean_session = false}, S) ->
    send_to_client(S, #'CONNACK'{return_code = ?CONNACK_IDENTIFIER_REJECTED}),
    prevent_connection(S,invalid_client_id);


%% Valid packet w/o Client Id
handle_packet(Packet = #'CONNECT'{client_id = <<>>,clean_session = true}, S) ->
    ClientId = auto_generate_client_id(),
    handle_packet(Packet#'CONNECT'{client_id = ClientId}, S);


%% Valid complete packet!
handle_packet(Packet = #'CONNECT'{},
              S = #state{connect_state = connecting}) ->
    NewState =
            try
                S1 = authorize(Packet,S),
                S2 = establish_session(Packet,S1),
                S3 = maybe_start_keep_alive(S2, Packet#'CONNECT'.keep_alive * 1000),
                S3#state{connect_state = connected}
            catch
                throw:{Reason,NewS}-> prevent_connection(NewS,Reason)
            end,
    {noreply,NewState};

%% Catch- all case
handle_packet(Packet, S = #state{connect_state = connecting})
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
     mqtt_session_out:pub_ack(SessionOut,PacketId),
    {noreply,S};

handle_packet(#'PUBREC'{packet_id = PacketId}, S = #state{session_out = SessionOut}) ->
    mqtt_session_out:pub_rec(SessionOut,PacketId),
%%     send_to_client(SenderPid,#'PUBREL'{packet_id = PacketId}),
    {noreply,S};

handle_packet(#'PUBCOMP'{packet_id = PacketId}, S = #state{session_out = SessionOut}) ->
     mqtt_session_out:pub_comp(SessionOut,PacketId),
    {noreply,S};

handle_packet(#'PUBREL'{packet_id = PacketId}, S = #state{session_in = SessionIn}) ->
    S1 = S#state{session_in = mqtt_publish:qos2_phase2(PacketId, SessionIn)},
    send_to_client(S1,#'PUBCOMP'{packet_id = PacketId}),
    {noreply,S1};

handle_packet(#'SUBSCRIBE'{packet_id = PacketId,subscriptions = Subs},
              S = #state{client_id = _ClientId,security = {Security,_},
                         session_out = SessionOut, auth_ctx = AuthCtx }) ->

    %%=======================================================================
    %% TODO: Use CleanSession to determine what to do
    %%=======================================================================
    %% Which subscriptions are we authorized to create?
    AuthResults = [{Security:authorize(AuthCtx,subscribe,Sub),Sub} || Sub  <- Subs],
    %% Actual subscriptions we are going to create
    AuthSubs = [Sub || {ok,Sub} <- AuthResults],
    SubResults = mqtt_session_out:subscribe(SessionOut, AuthSubs),
    %% Combine actual Subscription results with Authroization error results
    Results = combine_results([ Result || {Result,_} <- AuthResults],SubResults),
    Ack = #'SUBACK'{packet_id = PacketId,return_codes = Results},
    send_to_client(S,Ack),
    {noreply,S};


handle_packet(#'UNSUBSCRIBE'{packet_id = PacketId,topic_filters = Filters},
              S = #state{session_out = SessionOut}) ->
    mqtt_session_out:unsubscribe(SessionOut,Filters),
    send_to_client(S,#'UNSUBACK'{packet_id = PacketId}),
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
%% Connect
%% =================================================

authorize(#'CONNECT'{client_id = ClientId,
                     password = Password,
                     username = Username},
          S = #state{security = {Security,SecConf}}) ->
    %%=======================================================================
    %% @todo: authorize Will?!?!?
    %%=======================================================================
    case Security:authenticate(SecConf,ClientId,Username,Password) of
        {error,Reason} ->
            Code = case Reason of
                       bad_credentials -> ?CONNACK_BAD_USERNAME_OR_PASSWORD;
                       _               -> ?CONNACK_UNAUTHORIZED
                   end,
            send_to_client(S,#'CONNACK'{session_present = false,return_code = Code}),
            throw({bad_auth,S});
        {ok, AuthCtx} ->
            S#state{auth_ctx = AuthCtx}
    end.

establish_session(#'CONNECT'{client_id = ClientId,
                             clean_session = CleanSession,
                             will = Will},
                  S) ->
    SessionPresent = not CleanSession,
    NewState = S#state{clean_session = CleanSession,
                       client_id = ClientId,
                       session_in = mqtt_publish:new(ClientId,Will),
                       session_out = new_session(S,ClientId,CleanSession)},

    %% @todo:  Determine session present
    send_to_client(NewState, #'CONNACK'{return_code = ?CONNACK_ACCEPTED,
                                        session_present = SessionPresent}),
    NewState.

%% =================================================
%% Publish
%% =================================================

handle_publish(Packet,S) ->
    S#state{session_in = publish(Packet,S)}.

publish(Packet = #'PUBLISH'{packet_id = PacketId,
                            qos = QoS},
        #state{client_id     = ClientId,
               session_in    = SessionIn,
               sender_pid    = SenderPid}) ->

    Msg = map_publish_to_msg(Packet,ClientId),
    NewSessionIn = case QoS of
                       ?QOS_0 ->
                           mqtt_publish:qos0(Msg,SessionIn);
                       ?QOS_1 ->
                           Sess1 = mqtt_publish:qos1(Msg,SessionIn),
                           send_to_client(SenderPid, #'PUBACK'{packet_id = PacketId}),
                           Sess1;
                       ?QOS_2 ->
                           Sess2 = mqtt_publish:qos2_phase1(Msg,SessionIn),
                           send_to_client(SenderPid, #'PUBREC'{packet_id = PacketId}),
                           Sess2
                   end,
    NewSessionIn.

map_publish_to_msg(#'PUBLISH'{packet_id = PacketId,
                              retain    = Retain,
                              qos       = Qos,
                              content   = Content,
                              dup       = Dup,
                              topic     = Topic},
                   ClientId) ->

    #mqtt_message{packet_id = PacketId,
                  client_id = ClientId,
                  content   = Content,
                  dup       = Dup,
                  qos       = Qos,
                  retain    = Retain,
                  topic     = Topic}.


%%%===================================================================
%% SESSION interaction
%%%===================================================================

%% register_self(ClientId,_CleanSession) ->
%%     mqtt_reg_repo:register(ClientId),
%%     ok.
%%
%% unregister_self(ClientId,_CleanSession) ->
%%     mqtt_reg_repo:unregister(self(),ClientId),
%%     ok.

new_session(#state{sup_pid = SupPid, sender_pid = SenderPid},ClientId,CleanSession) ->
    mqtt_session_out:new(SupPid,ClientId,SenderPid,CleanSession).
%%     {ok,SessionPid} = mqtt_connection_sup2:create_session(SupPid,self(),CleanSession),
%%     SessionPid.

%% =================================================
%% Timer
%% =================================================

set_connect_timer(Timeout) ->
    timer:send_after(Timeout, connect_timeout).


maybe_start_keep_alive(S,0) -> S;
maybe_start_keep_alive(S,TimeOut) ->
    Ref = set_keep_alive_timer(TimeOut),
    S#state {keep_alive_ref = Ref,
             keep_alive_timeout = TimeOut}.


%% We use MatchRef to ensure we only receive current timeouts.
%% Thus we avoid some weird behavior if we happen to receive old timeout messages
%% that we have already canceled. Example:
%%
%% 1. We set the timeout to 5000 ms
%% 2. At 4999 ms we receive a new packet
%% 3. At 5000 ms the timeout message fires
%% 4. At 5001 ms We receive the packet:
%%         - RESET the timeout
%%         -Send back an ACK
%% 5. At 5002 ms we receive the OLD timeout
%% 5. At 5003 ms we close the connection, even though we just acked a packet 1ms ago!
set_keep_alive_timer(Timeout) ->
    MatchRef = make_ref(),
    TRef = timer:send_after(Timeout, {keep_alive_timeout,MatchRef}),
    {TRef,MatchRef}.


maybe_reset_keep_alive(S = #state{keep_alive_timeout = undefined}) -> S;
maybe_reset_keep_alive(S = #state{keep_alive_ref = Ref,
                                  keep_alive_timeout = TimeOut}) ->
    S#state{keep_alive_ref = reset_timer(Ref,TimeOut)}.

reset_timer({TRef,_},TimeOut) ->
    if TRef =/= undefned ->
        timer:cancel(TRef);
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

%% Common cleanup on bad disconnects
bad_disconnect(S = #state{session_in = SessionIn}) ->
    S1 = S#state{session_in = mqtt_publish:maybe_publish_will(SessionIn)},
    session_cleanup(S1).

session_cleanup(#state{session_out = SessionOut}) ->
    mqtt_session_out:close(SessionOut).

disconnect_client(_S,_Reason) ->
    ok. %% @todo: Do we even need this?


%% ==========================================================
%% Misc. heklper functions
%% ==========================================================

auto_generate_client_id() ->
    StrId = base64:encode_to_string(<<"__",(crypto:rand_bytes(24))/binary>>),
    list_to_binary(StrId).

combine_results(AuthResults, SubResults) ->
    lists:reverse(combine_results(AuthResults,SubResults,[])).

combine_results([], [], Acc) ->
    Acc;

combine_results([{error,_}|ART], SubResults, Acc) ->
    combine_results(ART,SubResults,[?SUBSCRIPTION_FAILURE|Acc]);

combine_results([ok|ART], [{ok,QoS}|SRT], Acc) ->
    combine_results(ART,SRT,[QoS|Acc]);

combine_results([ok|ART], [{error,_}|SRT], Acc) ->
    combine_results(ART,SRT,[?SUBSCRIPTION_FAILURE|Acc]).

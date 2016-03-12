%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% gen_server which hosts a mqtt_session data structure and handles side effects such as messaging and persistence
%%% @end
%%% Created : 24. Feb 2015 12:50 AM
%%%-------------------------------------------------------------------
-module(mqtt_session_out).
-author("Kalin").

-behaviour(gen_server).

%% API
-export([
    start_link/3,
    message_ack/2,
    message_pub_rec/2,
    message_pub_comp/2,
    subscribe/2,
    unsubscribe/2,
    %%cleanup/1
    push_qos0/2,
    push_reliable/3,
    close_duplicate/1,
    close/1,
    push_reliable_comp/3,
    new/4,
    subscription_created/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).



-define(SERVER, ?MODULE).

-record(state, {
    sender,
    client_id,
    seq,
    session,
    subs = orddict:new(),
    is_persistent,
    persist
}).

%%%===================================================================
%%% API
%%%===================================================================


new(SupPid,ClientId,SenderPid, CleanSession) ->
    {ok,SessionPid} = mqtt_connection_sup2:create_session(SupPid,SenderPid,ClientId,CleanSession),
    SessionPid.
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(ConnPid::pid(),ClientId::binary(),CleanSession::boolean()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(ConnPid,ClientId,CleanSession) ->
    gen_server:start_link(?MODULE, [ConnPid,ClientId,CleanSession], []).

push_qos0(Pid, CTRPacket) ->
    gen_server:cast(Pid,{push_0, CTRPacket}).

push_reliable(Pid, CTRPacket,QoS) ->
    gen_server:call(Pid,{push_reliable,CTRPacket,QoS}).

%%--------------------------------------------------------------------
%% @doc
%% Closes the connection if we detect two connections from the same ClientId
%% @end
%%--------------------------------------------------------------------
close_duplicate(Pid) ->
    gen_server:cast(Pid,{force_close, duplicate}).

close(Pid) ->
    gen_server:call(Pid,close).

push_reliable_comp(Pid,CTRPacket,QoS) ->
    gen_server:call(Pid,{push_reliable_comp,CTRPacket,QoS}).

message_ack(Pid,PacketId) ->
    gen_server:call(Pid,{ack,PacketId}).

message_pub_rec(Pid,PacketId) ->
    gen_server:call(Pid,{pub_rec,PacketId}).

message_pub_comp(Pid,PacketId) ->
    gen_server:call(Pid,{pub_comp,PacketId}).

subscribe(Pid,NewSubs) ->
    gen_server:call(Pid,{sub,NewSubs}).

unsubscribe(Pid,OldSubs) ->
    gen_server:call(Pid,{unsub,OldSubs}).

%%---------------------------------------------------------------------
%% @doc
%% Callback when a subscription has been created
%% @end
%%---------------------------------------------------------------------
subscription_created(Pid,Sub) ->
    gen_server:call(Pid,{sub_created,Sub}).

%% cleanup(Pid) ->
%%     gen_server:call(Pid,cleanup).

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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([ConnPid,ClientId,CleanSession]) ->
    self() ! {async_init,ClientId},
    Persist =
        if not CleanSession -> fun(SO) -> mqtt_session_repo:save(ClientId,SO),SO end;
           true         -> fun(SO) -> SO end
        end,
    S = #state{sender = ConnPid,
               is_persistent = not CleanSession,
               client_id = ClientId,
               persist = Persist},
    {ok,S}.

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

handle_call({push,CTRPacket}, _From,S = #state{session = SO,
                                               sender = Sender,
                                               persist = Persist}) ->
    {Result,SO2} =
        case mqtt_session:push_msg(CTRPacket,SO) of
            duplicate ->            %% do nothing
                {duplicate,SO};
            {full,SO1} ->
                Persist(SO1);
            {ok,Packet,SO1} ->    %% side effects
                Persist(SO1),
                send_to_client(Sender,Packet),
                {ok,SO1}
        end,
    {reply,Result,S#state{session = SO2}};

handle_call({ack,PacketId}, _From,  S = #state{session = SO,
                                               persist = Persist,
                                               client_id = ClientId}) ->
    SO2 =
        case mqtt_session:message_ack(PacketId,SO) of
            {ok,Notifs,SO1}  ->
                Persist(SO1),
                request_more(ClientId,Notifs),
                SO1;
            duplicate ->    SO
        end,
    {reply,ok,S#state{session = SO2}};

handle_call({pub_rec,PacketId}, _From,  S = #state{session = SO,
                                                   persist = Persist}) ->
    SO2 =
        case mqtt_session:message_pub_rec(PacketId,SO) of
            {ok,PubRec,SO1}  ->    Persist(SO1),SO1;
            duplicate ->    SO
        end,
    %% ALWAYS respond with PubRel
    Packet = mqtt_session:to_pubrel(PacketId),
    send_to_client(S,Packet),
    {reply,ok,S#state{session = SO2}};

handle_call({pub_comp,PacketId}, _From,  S = #state{client_id = ClientId,
                                                    session = SO,
                                                    persist = Persist}) ->
    SO2 =
        case mqtt_session:message_pub_comp(PacketId,SO) of
            {ok,Notifs,SO1}  ->
                Persist(SO1),
                request_more(ClientId,Notifs),
                SO1;
            duplicate -> SO
        end,
    {reply,ok,S#state{session = SO2}};

handle_call({sub,NewSubs}, _From,  S = #state{session = SO,
                                              client_id = ClientId,
                                              persist = Persist,
                                              seq = Seq}) ->
    SO1 = mqtt_session:subscribe(NewSubs,SO),
    QoSResults = [{ok,QoS} || {_,QoS} <- NewSubs], %% @todo: do we even need this?
    Persist(SO1),
    _Results = psubscribe(ClientId,Seq,NewSubs),
    {reply,QoSResults,S#state{session = SO1}};

handle_call({unsub,OldSubs}, _From,  S = #state{session = SO,
                                                client_id = ClientId,
                                                seq = Seq,
                                                persist = Persist}) ->
    SO1 = mqtt_session:unsubscribe(OldSubs,SO),
    Filters = [Filter || {Filter,_} <- OldSubs],
    punsubscribe(ClientId,Seq,Filters),
    Persist(SO1),
    {reply,ok,S#state{session = SO1}};

%% handle_call(cleanup, _From, S = #state{session_out = SO}) ->
%%     SO1 = mqtt_session:cleanup(SO),
%%     {reply,ok,S#state{session_out = SO1}};

handle_call(close, _From, S) ->
    {stop,normal,ok,S};

handle_call(Request, _From, State) ->
    error_logger:info_msg("Unmatched call ~p,~p~n",[Request,State]),
    {noreply, State}.

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

handle_cast({push_0,CTRPacket}, S) ->
    Packet = mqtt_session:to_publish(CTRPacket,0,undefined,false),
    send_to_client(S,Packet),
    {noreply,S};

handle_cast({force_close, _Reason}, S) ->
    %% cleanup(S),
    {stop, normal, S};

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


handle_info({async_init,ClientId}, S = #state{is_persistent = IsPersistent}) ->
    %%todo: should this be async? Do we want to send a CONNACK before clearing the session???
    error_logger:info_msg("Registering as ~p", [ClientId]),
    {Result,NewSeq} = mqtt_reg_repo:register_self(ClientId),
    %% Close duplicate registered Pids
    case Result of
        ok ->   ok;
        {dup_detected,DupPid} -> close_duplicate(DupPid)
    end,
    %% Either load an existing session of create a new one
    SO2 =
          case mqtt_session_repo:load(ClientId) of
              {error,not_found} -> mqtt_session:new();
              SO -> if  IsPersistent ->
                            SO;
                        true  ->
                            Filters = [Filter || {Filter,_} <- mqtt_session:get_subs(SO)],
                            punsubscribe(ClientId,NewSeq,Filters),
                            SO1 = mqtt_session:new(),
                            mqtt_session_repo:save(ClientId,SO1),
                            SO1
                    end
          end,
    %% Recover messages in flight and re-send them
    [send_to_client(S, Packet) || Packet <- mqtt_session:msg_in_flight(SO2)],
    Subs = mqtt_session:get_subs(SO2),
    psubscribe(ClientId,NewSeq,Subs),
    %% Refresh the subscriptions
    %%mqtt_router:refresh_subs(ClientId,NewSeq,mqtt_session:get_subs(SO2)),
    {noreply, S#state{seq = NewSeq,session = SO2}};

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

terminate(_Reason,S) ->
    cleanup(S).

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

psubscribe(ClientId,Seq,Subs) ->
    rpc:pmap({?MODULE,subscribe_self},[ClientId,Seq],Subs).

punsubscribe(ClientId,Seq,Filters) ->
    rpc:pmap({?MODULE,unsubscribe_self},[ClientId,Seq],Filters).

subscribe_self({Filter,QoS},ClientId,Seq) ->
    mqtt_router:subscribe(Filter,ClientId,QoS,Seq).

unsubscribe_self(Filter,ClientId,Seq) ->
    mqtt_router:unsubscribe(Filter,ClientId,Seq).

send_to_client(#state{sender = Sender}, Packet) ->
    send_to_client(Sender, Packet);

send_to_client(Sender, Packet) ->
    mqtt_sender:send_packet(Sender, Packet).

request_more(ClientId,Notifs) ->
    [mqtt_sub:request_more(Pid,ClientId,FromSeq,WSize) || [WSize,Pid,FromSeq] <- Notifs].

clear_session(#state{session = SO,client_id = ClientId},NewSeq) ->
    [mqtt_router:unsubscribe(Filter,ClientId,NewSeq)
        || {Filter,_} <- mqtt_session:get_subs(SO)],
    SO1 = mqtt_session:new(),
    mqtt_session_repo:save(ClientId,SO1),
    SO1.


maybe_clear_session(#state{is_persistent = true}) -> ok;

maybe_clear_session(#state{session = SO,client_id = ClientId,seq = Seq,is_persistent = false}) ->
    [mqtt_router:unsubscribe(Filter,ClientId,Seq) ||
        {Filter,_QoS}  <- mqtt_session:get_subs(SO)],
    ok.

apply_side_effects(SideEffects,SO) ->
    [apply_se(SE,SO) ||SE <- SideEffects].

apply_se({more,_Window},#state{client_id = _ClientId}) ->
    ok;

apply_se(persist,#state{persist = Persist,session = SO}) ->
    Persist(SO).

%% Termination handling
cleanup(S = #state{client_id = ClientId}) ->
    mqtt_reg_repo:unregister(ClientId),
    maybe_clear_session(S).



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
    pub_ack/2,
    pub_rec/2,
    pub_comp/2,
    subscribe/2,
    subscribe/4,
    unsubscribe/2,
    unsubscribe/3,
    push_qos0/2,
    push_reliable/3,
    close_duplicate/1,
    close/1,
    new/4,
    subscription_created/2,
    push/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).



-define(SERVER, ?MODULE).

-record(state, {
    sender          ::pid(),
    client_id       ::binary(),
    seq             ::non_neg_integer(),
    session         ::any(),
    monitors        ::dict:dict(reference(),binary()),
    is_persistent   ::boolean(),
    persist         ::fun()
}).

-define(DEFAULT_WSZIE,1).

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

push(Pid,Filter,Packet) ->
    gen_server:cast(Pid,{push,Filter,Packet}).

push_qos0(Pid, CTRPacket) ->
    gen_server:cast(Pid,{push_0,CTRPacket}).

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

pub_ack(Pid,PacketId) ->
    gen_server:call(Pid,{pub_ack,PacketId}).

pub_rec(Pid,PacketId) ->
    gen_server:call(Pid,{pub_rec,PacketId}).

pub_comp(Pid,PacketId) ->
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
    NewSeq = claim_client_id(ClientId),
    self() ! async_init,
    %%todo: should this be async? Do we want to send a CONNACK before clearing the session???
    IsPersistent = not CleanSession,
    Persist =
        if  IsPersistent -> fun(SO) -> mqtt_session_repo:save(ClientId,SO),SO end;
            true    -> fun(SO) -> SO end
        end,
    SO1 = load_session(ClientId,IsPersistent,NewSeq),
    S = #state{client_id = ClientId,
               seq = NewSeq,
               sender = ConnPid,
               is_persistent = not CleanSession,
               persist = Persist,
               session = SO1},
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

handle_call({pub_ack,PacketId}, _From,  S = #state{session = SO,
                                                   persist = Persist}) ->
    {ToSend,SO1} = mqtt_session:pub_ack(PacketId,SO),
    Persist(SO1),
    send_to_client(S,ToSend),
    {reply,ok,S#state{session = SO1}};

handle_call({pub_rec,PacketId}, _From,  S = #state{session = SO,
                                                   persist = Persist}) ->
    {ToSend,SO1} = mqtt_session:pub_rec(PacketId,SO),
    Persist(SO1),
    send_to_client(S,ToSend),
    {reply,ok,S#state{session = SO1}};

handle_call({pub_comp,PacketId}, _From,  S = #state{session = SO,
                                                    persist = Persist}) ->
    SO1 = mqtt_session:pub_comp(PacketId,SO),
    Persist(SO1),
    {reply,ok,S#state{session = SO1}};

handle_call({sub,NewSubs}, _From,  S = #state{session = SO,
                                              client_id = ClientId,
                                              persist = Persist,
                                              seq = Seq}) ->
    SO1 = mqtt_session:subscribe(NewSubs,SO),
    QoSResults = [{ok,QoS} || {_,QoS} <- NewSubs], %% @todo: do we even need this?
    Persist(SO1),
    _Results = p_subscribe(ClientId,Seq,NewSubs),
    {reply,QoSResults,S#state{session = SO1}};

handle_call({unsub,OldSubs}, _From, S = #state{session = SO,
                                               client_id = ClientId,
                                               seq = Seq,
                                               persist = Persist}) ->
    SO1 = mqtt_session:unsubscribe(OldSubs,SO),
    p_unsubscribe(ClientId,Seq,OldSubs),
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

handle_cast({push,Packet,Filter,{FromPid,Ref}},S = #state{session = SO,
                                                          persist = Persist,
                                                          client_id = ClientId}) ->
    {ToSend,SO1} = mqtt_session:push(Filter,Packet,SO),
    Persist(SO1),
    %% @todo: maybe combine the two casts into one???
    mqtt_router:ack(FromPid,ClientId,Ref),
    mqtt_router:pull(FromPid,1,ClientId),
    send_to_client(S,ToSend),
    {noreply,S#state{session = SO1}};

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

handle_info(async_init, S = #state{session = SO1,
                                   client_id = ClientId,
                                   seq = NewSeq}) ->
    Subs = mqtt_session:get_subs(SO1),
    MsgInFlight = mqtt_session:msg_in_flight(SO1),
    Mons = p_resume(ClientId,NewSeq,Subs),
    send_to_client(S,MsgInFlight),
    {noreply, S#state{session = SO1,
                      monitors = Mons}};

handle_info({'DOWN', MonRef, _, _, _}, S = #state{seq = CSeq,
                                                  client_id = ClientId,
                                                  monitors = Mons,
                                                  session = SO}) ->
    S1 =
        case dict:find(MonRef,Mons) of
            {ok,Filter} ->
                {ok,Sub} = mqtt_session:find_sub(Filter,SO),
                MonRef1 = resume(Sub,ClientId,CSeq,?DEFAULT_WSZIE),
                S#state{monitors = dict:store(Filter,MonRef1,Mons)};
            error -> S
        end,
    {noreply,S1};

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

claim_client_id(ClientId) ->
    error_logger:info_msg("Registering as ~p", [ClientId]),
    {Result,NewSeq} = mqtt_reg_repo:register_self(ClientId),
    %% Close duplicate registered Pids
    case Result of
        ok -> ok;
        {dup_detected,DupPid} -> close_duplicate(DupPid)
    end,
    NewSeq.


%% Either load an existing session of create a new one
load_session(ClientId,_IsPersistent = true,_) ->
    SO1 =
        case mqtt_session_repo:load(ClientId) of
            {error,not_found} ->
                mqtt_session:new();
            {ok,SO} -> SO
        end,
    mqtt_session_repo:save(ClientId,SO1);

load_session(ClientId,_IsPersistent = false,NewSeq) ->
    case mqtt_session_repo:load(ClientId) of
        {error,not_found} -> ok;
        {ok,SO} ->
            Filters = [Filter || {Filter,_,_} <- mqtt_session:get_subs(SO)],
            p_unsubscribe(ClientId,NewSeq,Filters)
    end,
    SO1 = mqtt_session:new(),
    mqtt_session_repo:save(ClientId,SO1),
    SO1.

p_subscribe(ClientId,Seq,Subs) ->
    rpc:pmap({?MODULE,subscribe},[ClientId,Seq,?DEFAULT_WSZIE],Subs).

subscribe({Filter,QoS},ClientId,CSeq,WSize) ->
    mqtt_router:subscribe(Filter,ClientId,QoS,CSeq,WSize).

p_unsubscribe(ClientId,CSeq,Filters) ->
    rpc:pmap({?MODULE,unsubscribe},[ClientId,CSeq],Filters).

unsubscribe(Filter,ClientId,Seq) ->
    mqtt_router:unsubscribe(Filter,ClientId,Seq).

p_resume(ClientId,CSeq,Subs) ->
    Mons = rpc:pmap({?MODULE,resume},[ClientId,CSeq,?DEFAULT_WSZIE],Subs),
    Filters = [Filter || {Filter,_,_} <- Subs],
    dict:from_list(lists:zip(Mons,Filters)).

resume({Filter,QoS,From},ClientId,CSeq,WSize) ->
    mqtt_router:resume_sub(ClientId,CSeq,{Filter,QoS,From},WSize).

send_to_client(#state{sender = Sender}, Packets) when is_list(Packets) ->
    lists:foreach(fun(P) -> send_to_client(Sender,P) end, Packets);

send_to_client(#state{sender = Sender}, Packet) ->
    send_to_client(Sender, Packet);

send_to_client(Sender, Packet) ->
    mqtt_sender:send_packet(Sender, Packet).

maybe_clear_session(#state{is_persistent = true}) -> ok;

maybe_clear_session(#state{is_persistent = false,
                           session = SO,
                           client_id = ClientId,
                           seq = Seq}) ->
    [mqtt_router:unsubscribe(Filter,ClientId,Seq) ||
        {Filter,_,_}  <- mqtt_session:get_subs(SO)],
    ok.

%% Termination handling
cleanup(S = #state{client_id = ClientId}) ->
    mqtt_reg_repo:unregister(ClientId),
    maybe_clear_session(S).



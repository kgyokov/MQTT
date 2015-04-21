%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Feb 2015 12:50 AM
%%%-------------------------------------------------------------------
-module(mqtt_session_out).
-author("Kalin").

-behaviour(gen_server).

%% API
-export([start_link/3, push_qos0/2, push_reliable/3, new/4, close_duplicate/1, close/1, push_reliable_comp/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([
         message_ack/2,
         message_pub_rec/2,
         message_pub_comp/2,
         subscribe/2,
         unsubscribe/2
         %%cleanup/1
]).

-define(SERVER, ?MODULE).

-record(state, {
    sender_pid,
    client_id,
    seq,
    session
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
    gen_server:cast(Pid, {force_close, duplicate}).

close(Pid) ->
    gen_server:call(Pid, close).

push_reliable_comp(Pid, CTRPacket,QoS) ->
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
    {ok, #state{sender_pid = ConnPid, session = mqtt_session:new(ClientId,CleanSession)}}.

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

handle_call({push_reliable, CTRPacket,QoS}, _From, S = #state{session = SO})
    when QoS =:= 1; QoS =:= 2 ->
    error_logger:info_msg("Pushing packet ~p with QoS = ~p~n",[CTRPacket,QoS]),
    case mqtt_session:append_msg(SO,CTRPacket,QoS) of
        duplicate ->
            {reply,duplicate,S#state{session = SO}};
        {ok,SO1,PacketId} ->
            maybe_persist(SO1),
            Packet = mqtt_session:to_publish(CTRPacket,QoS,PacketId,false),
            send_to_client(S,Packet),
            {reply,ok,S#state{session = SO1}}
    end;

handle_call({append_comp,Ref}, _From,  S = #state{session = SO}) ->
    SO1 = mqtt_session:append_message_comp(SO,Ref),
    {reply,ok,S#state{session = SO1}};

handle_call({ack,PacketId}, _From,  S = #state{session = SO}) ->
    NewSession =
    case mqtt_session:message_ack(SO,PacketId) of
        {ok,SO1}  ->    maybe_persist(SO1);
        duplicate ->    SO
    end,
    {reply,ok,S#state{session = NewSession}};

handle_call({pub_rec,PacketId}, _From,  S = #state{session = SO}) ->
    NewSession =
        case mqtt_session:message_pub_rec(SO,PacketId) of
            {ok,S01} ->   maybe_persist(S01);
            duplicate ->  SO
        end,
    %% ALWAYS respond with PubRel
    Packet = mqtt_session:to_pubrel(PacketId),
    send_to_client(S,Packet),
    {reply,ok,S#state{session = NewSession}};

handle_call({pub_comp,PacketId}, _From,  S = #state{session = SO}) ->
    NewSession =
        case mqtt_session:message_pub_comp(SO,PacketId) of
            {ok,SO1}    ->  maybe_persist(SO1);
            duplicate   ->  SO
        end,
    {reply,ok,S#state{session = NewSession}};

handle_call({sub,NewSub = {_,QoS}}, _From,  S = #state{session = SO}) ->
    SO1 = mqtt_session:subscribe(SO,[NewSub]),
    {reply,{ok,QoS},S#state{session = SO1}};

handle_call({unsub,OldSubs}, _From,  S = #state{session = SO}) ->
    SO1 = mqtt_session:unsubscribe(SO,OldSubs),
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
    cleanup(S),
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


handle_info({async_init,ClientId}, S) ->
    error_logger:info_msg("Registerin as ~p", [ClientId]),
    {Result,NewSeq} = mqtt_reg_repo:register(ClientId),
    case Result of
        ok ->
            ok;
        {dup_detected,DupPid} ->
            close_duplicate(DupPid)
    end,
    {noreply, S#state{seq = NewSeq}};

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
terminate(_Reason, #state{session = SO}) ->
    mqtt_session:cleanup(SO).

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

cleanup(#state{session = SO, client_id = ClientId}) ->
    mqtt_reg_repo:unregister(ClientId),
    mqtt_session:cleanup(SO).

maybe_persist(S) ->
    %% @todo: handle persistence
    S.

send_to_client(#state{sender_pid = SenderPid}, Packet) ->
    mqtt_sender:send_packet(SenderPid, Packet).
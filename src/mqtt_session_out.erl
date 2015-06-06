%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Hosts a mqtt_session data structure and handles side effects such as messaging and persistence
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
    new/4
]).

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
    is_persistent
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
    {ok, #state{sender = ConnPid, is_persistent = not CleanSession, client_id = ClientId}}.

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

handle_call({push_reliable,CTRPacket,QoS}, _From,S = #state{session = SO,
                                                            sender = Sender,
                                                            client_id = ClientId,
                                                            is_persistent = IsPersistent})
    when QoS =:= 1; QoS =:= 2 ->
    error_logger:info_msg("Pushing packet ~p with QoS = ~p~n",[CTRPacket,QoS]),
    {Result,SO2} =
        case mqtt_session:append_msg(SO,CTRPacket,QoS) of
            duplicate ->            %% do nothing
                {duplicate,SO};
            {ok,SO1,PacketId} ->    %% side effects
                maybe_persist(SO1,ClientId,IsPersistent),
                Packet = mqtt_session:to_publish(CTRPacket,false,QoS,PacketId,false),
                send_to_client(Sender,Packet),
                {ok,SO1}
        end,
    {reply,Result,S#state{session = SO2}};

handle_call({append_comp,Ref}, _From,  S = #state{session = SO}) ->
    SO1 = mqtt_session:append_message_comp(SO,Ref),
    {reply,ok,S#state{session = SO1}};

handle_call({ack,PacketId}, _From,  S = #state{session = SO,
                                               client_id = ClientId,
                                               is_persistent = IsPersistent}) ->
    SO2 =
    case mqtt_session:message_ack(SO,PacketId) of
        {ok,SO1}  ->    maybe_persist(SO1,ClientId,IsPersistent);
        duplicate ->    SO
    end,
    {reply,ok,S#state{session = SO2}};

handle_call({pub_rec,PacketId}, _From,  S = #state{session = SO,
                                                   client_id = ClientId,
                                                   is_persistent = IsPersistent}) ->
    SO2 =
        case mqtt_session:message_pub_rec(SO,PacketId) of
            {ok,SO1}  ->    maybe_persist(SO1,ClientId,IsPersistent);
            duplicate ->    SO
        end,
    %% ALWAYS respond with PubRel
    Packet = mqtt_session:to_pubrel(PacketId),
    send_to_client(S,Packet),
    {reply,ok,S#state{session = SO2}};

handle_call({pub_comp,PacketId}, _From,  S = #state{session = SO,
                                                    client_id = ClientId,
                                                    is_persistent = IsPersistent}) ->
    SO2 =
        case mqtt_session:message_pub_comp(SO,PacketId) of
            {ok,SO1}  ->    maybe_persist(SO1,ClientId,IsPersistent);
            duplicate ->    SO
        end,
    {reply,ok,S#state{session = SO2}};

handle_call({sub,NewSubs}, _From,  S = #state{session = SO,
                                              sender = Sender,
                                              client_id = ClientId,
                                              is_persistent = IsPersistent}) ->
    QosResults = [{ok,QoS} || {_,QoS} <- NewSubs],
    Filters = [Filter || {Filter,_} <- NewSubs],

    %% Add subscriptions to in-memory session
    error_logger:info_msg("Subscribing to ~p~n",[NewSubs]),
    SO1 = mqtt_session:subscribe(SO,NewSubs),
    [mqtt_sub_repo:add_sub(ClientId,Filter,QoS) || {Filter,QoS} <- NewSubs],

    %% Get the retained messages
    Msgs =
    [   begin
          {_,SubQos} = mqtt_topic:best_match(NewSubs,Topic),
          {Topic,Content,Ref,min(SubQos,MsgQoS)}
        end
      ||{Topic,Content,Ref,MsgQoS} <- mqtt_topic_repo:get_retained(Filters)],
    %% Apply them to session
    {Results,SO3} = lists:mapfoldl(
        fun(Msg,SOAcc) ->
            {Topic,Content,Ref,QoS} = Msg,
            CTRPacket = {Topic,Content,Ref},
            case mqtt_session:append_msg(SO1,CTRPacket,QoS) of
                duplicate ->            {duplicate,SOAcc};
                {ok,SO2,PacketId} ->    {{ok,CTRPacket,QoS,PacketId},SO2}
            end
        end,
        SO1,Msgs),
    maybe_persist(SO3,ClientId,IsPersistent),
    %% Send any that need to be sent
    [   case Result of
              {ok,CTRPacket,QoS,PacketId} ->
                  Packet = mqtt_session:to_publish(CTRPacket,true,QoS,PacketId,false),
                  send_to_client(Sender,Packet);
              _ ->
                  ok
        end
      || Result <- Results],
    {reply,QosResults,S#state{session = SO3}};

handle_call({unsub,OldSubs}, _From,  S = #state{session = SO,
                                                client_id = ClientId,
                                                is_persistent = IsPersistent}) ->
    SO1 = mqtt_session:unsubscribe(SO,OldSubs),
    [mqtt_sub_repo:remove_sub(ClientId,Topic) || Topic <- OldSubs],
    maybe_persist(SO1,ClientId,IsPersistent),
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
    Packet = mqtt_session:to_publish(CTRPacket,false,0,undefined,false),
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
    error_logger:info_msg("Registerin as ~p", [ClientId]),
    {Result,NewSeq} = mqtt_reg_repo:register(ClientId),
    %% Close duplicate registered Pids
    case Result of
        ok ->   ok;
        {dup_detected,DupPid} -> close_duplicate(DupPid)
    end,
    %% Either load an existing session of create a new one
    SO1 = if IsPersistent ->
              case mqtt_session_repo:load(ClientId) of
                  {error,not_found} -> mqtt_session:new();
                  SO -> SO
              end;
            true ->
                mqtt_session:new()
          end,
    %% Recover msessages in flight and re-send them
    [send_to_client(S, Packet) || Packet <- mqtt_session:msg_in_flight(SO1)],
    {noreply, S#state{seq = NewSeq,session = SO1}};

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
    cleanup(S),
    S.

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

%% apply_to_session(S = #state{session = SO,
%%                             is_persistent = IsPersistent,
%%                             sender = Sender},Fun) ->
%%     SO2 =
%%         case Fun(SO) of
%%             {persist,SO1} ->
%%                 maybe_persist(SO1,IsPersistent),
%%                 SO1;
%%             {persist,SO1,Packets} ->
%%                 maybe_persist(SO1,IsPersistent),
%%                 [send_to_client(Sender,Packet)|| Packet <-Packets],
%%                 SO1;
%%             {ok,SO1,Packets} ->
%%                 maybe_persist(SO1,IsPersistent),
%%                 [send_to_client(Sender,Packet)|| Packet <-Packets],
%%                 SO1;
%%             duplicate ->
%%                 SO
%%         end,
%%     S#state{session = SO2}.

maybe_persist(SO,_ClientId,false) ->
    SO;

maybe_persist(SO,ClientId,true) ->
    mqtt_session_repo:save(ClientId,SO),
    SO.

send_to_client(#state{sender = Sender}, Packet) ->
    send_to_client(Sender, Packet);

send_to_client(Sender, Packet) ->
    mqtt_sender:send_packet(Sender, Packet).

%% Termination handling
cleanup(#state{session = SO,client_id = ClientId,is_persistent = IsPers}) ->
    mqtt_reg_repo:unregister(ClientId),
    if not IsPers ->
        Subs = mqtt_session:get_subs(SO),
        [mqtt_sub_repo:remove_sub(ClientId,Topic) || {Topic,_QoS}  <- Subs];
      true -> ok
    end.

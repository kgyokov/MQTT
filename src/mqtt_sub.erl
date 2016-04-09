%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Handles subscriptions for a particular filter.
%%% Loads subscribed Clients and (potentially) their Pids from a Database at initialization time.
%%% Monitors the Pids of those Clients and disposes of them when the Clients go down.
%%% When a Client process comes back up, it is expected to re-subscribe
%%% @end
%%% Created : 06. Nov 2015 10:05 PM
%%%-------------------------------------------------------------------
-module(mqtt_sub).
-author("Kalin").

-include("mqtt_internal_msgs.hrl").

-define(MONOID,sequence_monoid).

-behaviour(gen_server).

%% API
-export([start_link/2,
    %% Subs
    subscribe_self/5,
    cancel/3,
    %% Packets
    pull/3,
    ack/3,
    push/2,
    new/1, resume/6]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SUPERVISOR, mqtt_sub_sup).
-define(REPO, mqtt_sub_repo).

-type dict()::any().

-record(state, {
    repo            ::module(), %% Module used for persistence
    filter          ::binary(), %% Filter handled by this process (e.g. /A/B/+
    live_subs       ::dict(),    %% dictionary of {ClientId::binary(),#client_reg{}}
    dead_subs       ::dict(),
    monref_idx      ::any(),    %% dictionary of (MonitorRef,ClientId}
    packet_seq      ::non_neg_integer(),%% A sequence number assigned to each Message from a topic covered by this filter.
                                    %% This process assigns incremental integers to each new message
    queue           ::any(),    %% Shared queue of messages
    retained        ::shared_set:shared_set(),   %% Dictionary of retained messages
    garbage         ::any()     %% garbage stats from the queue
}).

-record(sub, {
    monref              ::any(), %% monitor reference for the process handling the client
    pid                 ::pid(), %% Process id of the process handling the client
    client_seq = 0      ::non_neg_integer() %% the version number of the client process registration
                                        %% (incremented every time a new client process is spawned, used to choose
                                        %% between different instances of a Client in case of race conditions)
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
-spec(start_link(Filter::binary(),Repo::module()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Filter,Repo) ->
    gen_server:start_link(?MODULE, [Filter,Repo], []).



%%%===================================================================
%%% Publisher API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a Client subscription
%% @end
%%--------------------------------------------------------------------
-spec(subscribe_self(Pid::pid(),ClientId::client_id(),CSeq::non_neg_integer(),
                     QoS::qos(),WSize::non_neg_integer())
        -> ok).
subscribe_self(Pid,ClientId,CSeq,QoS,WSize) ->
    gen_server:call(Pid,{sub,ClientId,CSeq,QoS,WSize}).

-spec(resume(Pid::pid(), ClientId::client_id(), CSeq::non_neg_integer(),
             Qos::qos(), any(), WSize::non_neg_integer())
        -> ok).
resume(Pid,ClientId,CSeq,QoS,ResumeFrom,WSize) ->
    gen_server:call(Pid,{resume,ClientId,CSeq,QoS,ResumeFrom,WSize}).


%%%===================================================================
%%% Subscription API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cancels a Client subscription
%% @end
%%--------------------------------------------------------------------
-spec(cancel(Pid::pid(),ClientId::client_id(),Seq::non_neg_integer())
        -> ok).
cancel(Pid,ClientId,Seq) ->
    gen_server:call(Pid,{unsub,ClientId,Seq}).


%%%===================================================================
%%% 'Other' API
%%%===================================================================

push(Pid,Packet) ->
    gen_server:call(Pid,{push,Packet}).

pull(Pid,WSize,ClientId) ->
    gen_server:cast(Pid,{pull,WSize,ClientId}).

ack(Pid,ClientId,Ack) ->
    gen_server:cast(Pid,{ack,ClientId,Ack}).

%% Hides the supervisor
new(Filter) ->
    ?SUPERVISOR:start_sub(Filter).


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
init([Filter, Repo]) ->
    self() ! async_init,
    mqtt_sub_repo:claim_filter(Filter,self()),
    Seq = 0,
    {ok,#state{filter = Filter,
               monref_idx = dict:new(),
               live_subs = dict:new(),
               dead_subs = dict:new(),
               packet_seq = Seq,
               queue = shared_queue:new(Seq),
               retained = shared_set:new(),
               repo = Repo}}.

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


handle_call({sub,ClientId,CSeq,QoS,WSize},{Pid,_},S = #state{filter = Filter,
                                                             live_subs = Subs}) ->
    NewSub = {ClientId,CSeq,QoS,Pid},
    case dict:find(ClientId,Subs) of
        error ->
            mqtt_sub_repo:save_sub(Filter,NewSub),
            S1 = insert_new_sub(NewSub,WSize,S),
            {reply,ok,S1};
        {ok,Sub} ->
            case Sub of
                #sub{client_seq = CurSeq} when CurSeq > CSeq ->
                    %% Message from an old process. Ignore it!
                    %% This can happen in the following (unlikely, but theoretically possible) scenario:
                    %% 1. Process A is spawned for this ClientId and sends a 'sub' message to this process
                    %% 2. Process A dies
                    %% 3. Process B is spawned for the same ClientId and sends another 'sub' message
                    %% 4. 'sub' message from process A is received and this process starts to monitor process A
                    %% 5. Because the 'monitor' call is asynchronous, 'sub' message from process B is received
                    %%      while this process still thinks process A is alive
                    {reply,duplicate,S};
                #sub{pid = OldPid,monref = OldRef} ->
                    mqtt_sub_repo:save_sub(Filter,NewSub),
                    S1  =
                        case OldPid =:= Pid of
                            true -> replace_monitor(Pid,OldRef,S);
                             _-> S
                        end,
                    S2 = resubscribe(ClientId,QoS,NewSub,S1),
                    {reply,ok,S2}
            end
    end;

handle_call({resume,ClientId,CSeq,QoS,ResumeFrom,WSize},{Pid,_},S = #state{monref_idx = Mon,
                                                                           live_subs = Subs,
                                                                           queue = SQ,
                                                                           retained = Ret}) ->
    case dict:find(ClientId,Subs) of
        error -> ok;
        {ok,Sub = #sub{monref = MonRef}} ->
            SubInfo = {ClientId,CSeq,QoS},
            mqtt_sub_state:resume(Pid,MonRef,ResumeFrom,WSize,SQ,Ret,Sub)
    end;

handle_call({unsub,ClientId,Seq}, _From, S = #state{filter = Filter,
                                                    monref_idx = Mon,
                                                    live_subs = Subs,
                                                    queue = SQ,
                                                    garbage = Garbage}) ->
    case dict:find(ClientId,Subs) of
        error ->
            {reply,ok,S};
        {ok,Sub} ->
            case Sub of
                #sub{monref = MonRef,client_seq = OldSeq} when Seq >= OldSeq ->
                    mqtt_sub_repo:remove_sub(Filter,ClientId),
                    Mon1 = maybe_demonitor_client(MonRef,Mon),
                    Subs1 = dict:erase(ClientId,Subs),
                    {MoreGarbage,SQ1} = shared_queue:remove(ClientId,SQ),
                    S1 = S#state{monref_idx = Mon1,
                                 live_subs = Subs1,
                                 queue = SQ1,
                                 garbage = ?MONOID:as(Garbage,MoreGarbage)},
                    case dict:size(Subs1) of
                        0 -> {stop,no_subs,ok,S1};
                        _ -> {reply,ok,S1}
                    end;
                _ ->
                    {reply,ok,S}
            end
    end;


handle_call({push,Packet}, From, S) ->
    handle_new_packet(Packet,From,S);

%%handle_call(get_live_clients, _From, S = #state{live_subs = Subs}) ->
%%    Pids = [
%%        {ClientId,QoS,Pid} ||
%%        {ClientId,#sub{pid = Pid,qos = QoS}} <- dict:to_list(Subs), Pid =/= undefined],
%%    {reply, Pids, S};

handle_call(_Request, _From, State) ->
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

handle_cast({pull,WSize,ClientId}, S) ->
    {noreply,handle_pull(WSize,ClientId,S)};

handle_cast({ack,ClientId,Ack}, S) ->
    {noreply,handle_ack(ClientId,Ack,S)};

handle_cast(_Request, S) ->
    {noreply, S}.

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

handle_info({'DOWN', MonRef, _, _, _}, S) ->
    {noreply, maybe_remove_downed(MonRef,S)};

handle_info(async_init, S = #state{filter = Filter}) ->
    RepoSubs = mqtt_sub_repo:load(Filter),
    S1 = recover(RepoSubs,S),
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
terminate(_Reason, S = #state{filter = Filter}) ->
    error_logger:info_msg("Terminating Sub with id ~p, state ~p, reason ~p",[self(),S,_Reason]),
    mqtt_sub_repo:unclaim_filter(Filter,self()).

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

handle_new_packet(Packet,_From,S = #state{filter = Filter,
                                          live_subs = Subs,
                                          retained = Ret,
                                          queue = SQ}) ->
    SQ1 = shared_queue:pushr({Packet#packet.qos,Packet},SQ),
    NewSeq = shared_queue:max_seq(SQ1),
    Ret1 = maybe_store_retained(Packet,NewSeq,Ret),
    {SendTo,Subs1} = dict:fold(update_waiting_subs(NewSeq),{[],Subs},Subs),
    S1 = S#state{live_subs = Subs1,
                 queue = SQ1,
                 retained = Ret1},
    MQPacket = Packet#packet{retain = false, ref = {q,NewSeq}},
    send_packet_to_clients(SendTo,Filter,MQPacket),
    {reply,{ok, NewSeq},S1}.

update_waiting_subs(NewSeq) ->
    fun(ClientId,Sub,AccIn = {SendToAcc,SubsAcc}) ->
            case mqtt_sub_state:maybe_update_waiting(NewSeq,Sub) of
                {ok,Sub1 = #sub{pid = Pid}} ->
                    SubsAcc1 = dict:update(ClientId,Sub1,SubsAcc),
                    {[Pid|SendToAcc],SubsAcc1};
                _ -> AccIn
            end
    end.


handle_pull(WSize,ClientId,S = #state{filter = Filter,
                                      live_subs = Subs,
                                      queue = SQ}) ->
    Fun =
        fun(Sub = #sub{pid = Pid}) ->
            {ok,Packets,Sub1} = mqtt_sub_state:take(WSize,SQ,Sub),
            send_packets_to_client(Pid,Filter,Packets),
            Sub1
        end,
    Subs1 = dict:update(ClientId,Fun,Subs),
    S#state{live_subs = Subs1}.

%%handle_ack(ClientId,{ret,RetAck},S = #state{live_subs = Subs}) ->
%%    Subs1 = dict:update(ClientId,
%%        fun(Sub) ->
%%            mqtt_sub_state:ack_retained(RetAck,Sub)
%%        end,
%%        Subs),
%%    S#state{live_subs = Subs1};

handle_ack(ClientId,{q,QAck},S = #state{queue = Q,
                                        garbage = Garbage}) ->
    {MoreGarbage,Q1} = shared_queue:forward(ClientId,QAck,Q),
    S#state{queue = Q1,garbage = ?MONOID:as(Garbage,MoreGarbage)}.

replace_monitor(Pid,OldRef,S = #state{monref_idx = Mon}) ->
    NewRef = monitor(process,Pid),
    MonRef = maybe_demonitor_client(OldRef,Mon),
    MonRef1 = dict:store(NewRef,Pid,MonRef),
    S#state{monref_idx = MonRef1}.


%% ===================================================================
%% Packet Sending
%% ===================================================================

send_packet_to_clients(CPids,Filter,Packet) ->
    [send_packet(CPid,Filter,Packet) || CPid <- CPids].

send_packets_to_client(CPid,Filter,Packets) ->
    [send_packet(CPid,Filter,P) || P <- Packets].

send_packet(CPid,Filter,P) -> mqtt_session_out:push(CPid,Filter,P).

%% ===================================================================
%% Subscriptions
%% ===================================================================

set_sub({ClientId,ClientSeq,QoS,Pid},NextInQ,WSize,S = #state{monref_idx = Mons,
                                                              live_subs = Subs,
                                                              queue = SQ,
                                                              retained = Ret}) ->
    MonRef = monitor(process,Pid),
    Sub = mqtt_sub_state:new(Pid,MonRef,ClientSeq,QoS,NextInQ,Ret,WSize),
    S#state{live_subs = dict:store(ClientId,Sub,Subs),
            monref_idx = dict:store(MonRef,ClientId,Mons),
            queue = shared_queue:add(ClientId,SQ)}.

%%
%% A client comes back online and re-establishes an existing subscription
%%
recover_sub({ClientId,_QoS,_CSeq,_ClientPid},Q) ->
    shared_queue:add(ClientId,undefined,Q).

insert_new_sub(SubInfo,WSize,S = #state{queue = SQ}) ->
    NextInQ = shared_queue:max_seq(SQ), %% start reading packets from the next one
    set_sub(SubInfo,NextInQ,WSize,S).

%%
%% Only update existing properties - this is not a new subscription
%%
resubscribe(ClientId,QoS,Sub,S = #state{retained = Ret,live_subs = Subs}) ->
    Sub1 = mqtt_sub_state:resubscribe(QoS,Ret,Sub),
    S#state{live_subs = dict:store(ClientId,Sub1,Subs)}.

maybe_store_retained(#packet{retain = false},_Seq,Ret)               -> Ret;
maybe_store_retained(#packet{topic = Topic,content = <<>>},Seq,Ret)  -> shared_set:remove(Topic,Seq,Ret);
maybe_store_retained(Packet = #packet{topic = Topic},Seq,Ret)        -> shared_set:append(Topic,Packet,Seq,Ret).

%% =====================================================================
%% Monitoring and Recovery
%% =====================================================================

maybe_remove_downed(MonRef,S = #state{filter = Filter,
                                      live_subs = Subs,
                                      monref_idx = Mon}) ->
    case dict:find(MonRef,Mon) of
        {ok,ClientId} ->
            mqtt_sub_repo:clear_sub_pid(Filter,ClientId),
            Subs1 = dict:update(ClientId,
                        fun(Reg) -> Reg#sub{monref = undefined,
                                            pid = undefined}
                        end,
                    Subs),
            Mon1 = dict:erase(MonRef,Mon),
            S#state{monref_idx = Mon1,live_subs = Subs1};
        error -> S
    end.

recover(SubRecord,S = #state{queue = Q}) ->
    lists:foldl(fun recover_sub/2,Q,SubRecord).

maybe_clean_garbage(Garbage) ->
    %%@todo: perform cleanup
    ok.

monitor_client(Pid,Mon) ->
    Ref = monitor([process],Pid).


maybe_demonitor_client(undefined,Mons) ->
    Mons;
maybe_demonitor_client(Ref,Mons)       ->
    demonitor(Ref,[flush]),
    dict:erase(Ref,Mons).

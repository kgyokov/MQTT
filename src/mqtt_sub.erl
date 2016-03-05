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

-behaviour(gen_server).

%% API
-export([start_link/2,
    subscribe_self/4,
    cancel/3,
    request_more/4,
    push/3,
    get_live_clients/1,
    new/1]).

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
    queue ::any(), %% Shared queue of messages
    min_acks        ::any() %% maintains the minimum message seq processed by any opf the clients
}).

-record(sub, {
    qos         ::qos(), %% QoS for this client subscription
    monref      ::any(), %% monitor reference for the process handling the client
    pid         ::pid(), %% Process id of the process handling the client
    client_seq = 0 ::non_neg_integer(), %% the version number of the client process registration
                                        %% (incremented every time a new client process is spawned, used to choose
                                        %% between different instances of a Client in case of race conditions)
    last_ack = 0  ::non_neg_integer(),  %% the filter-assigned sequence number of the last message processed by this client,
    last_sent = 0 ::non_neg_integer(),  %% the last message sent to the client
    requested = 0 ::non_neg_integer()   %% How many messages the client has requested
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
-spec(subscribe_self(Pid::pid(),ClientId::client_id(),QoS::qos(),Seq::non_neg_integer())
        -> ok).
subscribe_self(Pid,ClientId,QoS,Seq) ->
    gen_server:call(Pid,{sub,ClientId,QoS,Seq}).


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

-spec(get_live_clients(Pid::pid()) ->
    [{ClientId::client_id(),QoS::qos(),Pid::pid()}]).
get_live_clients(Pid) ->
    gen_server:call(Pid,get_live_clients).

push(Pid,Packet,QoS) ->
    gen_server:call(Pid,{push,Packet,QoS}).

request_more(Pid,ClientId,FromSeq,WSize) ->
    gen_server:call(Pid,{request_more,ClientId,FromSeq,WSize}).

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
    Seq = 0,
    {ok,#state{ filter = Filter,
                monref_idx = dict:new(),
                live_subs = dict:new(),
                dead_subs = dict:new(),
                packet_seq = Seq,
                min_acks = min_val_tree:new(),
                queue = shared_queue:new(Seq),
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


handle_call({sub,ClientId,QoS,CSeq},{Pid,_},S = #state{filter = Filter,
                                                       live_subs = Subs,
                                                       monref_idx = Mon,
                                                       packet_seq = PSeq}) ->
    Client = dict:find(ClientId,Subs),
    case Client of
        error ->
            NewSub = {ClientId,QoS,CSeq,PSeq,Pid},
            mqtt_sub_repo:save_sub(Filter,NewSub),
            S1 = set_sub(NewSub,S),
            {reply,{ok,PSeq},S1};
        {ok, Reg} ->
            case Reg of
                #sub{pid = OldPid, client_seq = CurSeq} when CurSeq > CSeq ->
                    %%ignore old messages from this Pid
                    Resp = if OldPid =:= Pid; OldPid =:= undefined -> {ok,PSeq};
                            %% this is an old process, not interested in messages
                               true -> duplicate
                           end,
                    {reply,Resp,S};
                #sub{pid = Pid,qos = QoS} ->
                    %% Pid and QoS are the unchanged
                    {reply,{ok, PSeq},S};
                #sub{pid = Pid,qos = OldQoS} ->
                    %% replacing the QoS
                    NewSub = {ClientId,QoS,CSeq, PSeq,Pid},
                    mqtt_sub_repo:save_sub(Filter,NewSub),
                    Reg1 = Reg#sub{qos = QoS,client_seq = CSeq},
                    Cs1 = dict:store(ClientId, Reg1, Subs),
                    S1 = S#state{live_subs = Cs1},
                    {reply,{ok, PSeq},S1};
                #sub{monref = OldRef, pid = OldPid} when OldPid =/= Pid ->
                    %% replacing the Pid and QoS
                    NewSub = 1,
                    mqtt_sub_repo:save_sub(Filter,NewSub),
                    Mon1 = maybe_demonitor_reg(OldRef,Mon),
                    S1 = set_sub(NewSub,S#state{monref_idx = Mon1}),
                    {reply,{ok, PSeq},S1}
            end
    end;

handle_call({unsub,ClientId,Seq}, _From, S = #state{live_subs = Subs}) ->
    case dict:find(ClientId, Subs) of
        error ->
            {reply,ok,S};
        {ok, Reg} ->
            case Reg of
                #sub{monref = MonRef,client_seq = OldSeq} when Seq >= OldSeq ->
                    mqtt_sub_repo:remove_sub(S#state.filter,ClientId),
                    #state{monref_idx = Mon, min_acks = MAck} = S,
                    Mon1 = maybe_demonitor_reg(MonRef,Mon),
                    Subs1 = dict:erase(ClientId,Subs),
                    Min = min_val_tree:min(MAck),
                    MAcks1 = min_val_tree:remove(ClientId, MAck),
                    Min1 = min_val_tree:min(MAcks1),
                    S1 = S#state{monref_idx = Mon1,
                                 live_subs = Subs1,
                                 min_acks = MAcks1},
                    case dict:size(Subs1) of
                        0 -> {stop, no_subs,ok,S1};
                        _ -> {reply,ok,S1}
                    end;
                _ ->
                    {reply,ok,S}
            end
    end;


handle_call({push,Packet,QoS}, From, S) ->
    handle_new_packet(Packet,QoS,From,S);

handle_call(get_live_clients, _From, S = #state{live_subs = Subs}) ->
    Pids = [
        {ClientId,QoS,Pid} ||
        {ClientId,#sub{pid = Pid,qos = QoS}} <- dict:to_list(Subs), Pid =/= undefined],
    {reply, Pids, S};

handle_call({request_more,ClientId,FromSeq,WSize}, _From, S) ->
    handle_request_for_more(ClientId,FromSeq,WSize,S);

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

handle_info({'DOWN', MonRef, _, _, _}, S) ->
    S1 = maybe_remove_downed(MonRef,S),
    {noreply, S1};

handle_info(async_init, S = #state{filter = Filter,repo = Repo}) ->
    SubState = Repo:load(Filter),
    S1 = recover(S,SubState),
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
terminate(_Reason, S = #state{filter = Filter,repo = Repo}) ->
    error_logger:info_msg("Terminating Sub with id ~p, state ~p, reason ~p",[self(),S,_Reason]),
    Repo:clear(Filter).

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

%%
%% The following invariants are maintained:
%% last_ack =< last_sent =< msg_seq =< requested = last_ack + Client Window
%% where:
%%    last_ack is the last message acknowledged by the client
%%    last_sent is the last message sent to the client
%%    msg_seq is the last message received by the sub
%%    request is the maximum message acceptable by the client (last_ack + Client Window)
%% Therefore:
%% requested = last_ack + Wnd
%% last_sent = min(requested,msg_seq)
%%

handle_new_packet(Packet,QoS,_From,S = #state{live_subs = Subs,
                                              queue = SQ}) ->

    SQ1 = shared_queue:pushr(Packet,SQ),
    PSeq1 = shared_queue:max_seq(SQ1),
    {PushTo,Subs1} = dict:fold(fun(ClientId,Sub,AccIn = {PushToAcc,SubsAcc}) ->
                                    case maybe_update_waiting_sub(PSeq1,Sub) of
                                        {ok,Sub1 = #sub{pid = Pid}} ->
                                            SubsAcc1 = dict:update(ClientId,Sub1,SubsAcc),
                                            {[Pid|PushToAcc],SubsAcc1};
                                        _ -> AccIn
                                    end
                              end,
                    {[],Subs},Subs),
    S1 = S#state{packet_seq = PSeq1,
                 live_subs = Subs1,
                 queue = SQ1},
    send_packet_to_clients(PushTo,Packet,QoS),
    {reply,{ok,PSeq1},S1}.


%% @doc
%% Updates a sub if the new packet sequence is [Last Sent ... Requested] window
%% @end
maybe_update_waiting_sub(PSeq,Sub = #sub{requested = Req})
    when PSeq =< Req ->
    Sub1 = Sub#sub{last_sent = PSeq},
    %%Range = {LSent,PSeq},
    %%{ok,Sub1,Range};
    {ok,Sub1};

maybe_update_waiting_sub(_,_) -> ignore.

handle_request_for_more(ClientId,RequestFrom,WSize,S = #state{live_subs = Subs,
                                                              queue = SQ}) ->
    S1 = case dict:find(ClientId,Subs) of
            {ok,Sub = #sub{pid = Pid}} ->
                PSeq = shared_queue:max_seq(SQ),
                {ok,Sub1,{From,To}} = packet_range_to_send(PSeq,WSize,Sub),
                ActualFrom = max(From,shared_queue:min_seq(SQ)),
                ActualTo = min(To,shared_queue:max_seq(SQ)),
                Packets = shared_queue:read(ActualFrom,ActualTo,SQ),
                S2 = S#state{live_subs = dict:update(ClientId,Sub1,Subs)},
                %%queue = shared_queue:move(ClientId,ActualTo,SQ)},
                send_packets(Packets,Pid,S2),
                S2;
            error ->
                S
         end,
    {ok,{reply,ok},S1}.

handle_ack(ClientId,AckSeq,S = #state{queue = SQ}) ->
    S#state{queue = shared_queue:move(ClientId,AckSeq,SQ)}.


%%maybe_process_pending1(PSeq,Ack,Wnd,Sub = #sub{last_sent = LSent,
%%    last_ack = LAck,
%%    requested = Req}) ->
%%    LAck1 = max(Ack,LAck),
%%    Req1 = max(Req,Ack+Wnd),
%%    maybe_process_pending(PSeq,LAck1,Req1,Sub).

%% @doc
%% Determine the sequence range of packets to send depending on the size of the Client's Window
%% @end
packet_range_to_send(PSeq,WSize,Sub = #sub{last_sent = LastSent,
                                           requested = Req}) ->
    %% sanitize inputs
%%    LAck1 = max(Ack,LAck),
%%    Req1 = max(Req,LAck+WSize),
    Req1 = max(Req,LastSent+WSize),
    SendUntil = min(Req,PSeq),
    Sub1 = Sub#sub{last_sent = SendUntil,
                   requested = Req1},
    Range = {LastSent,SendUntil},
    {ok,Sub1,Range};

packet_range_to_send(_,_,_) -> ignore.

%% ===================================================================
%% Packet Sending
%% ===================================================================

send_packet_to_clients(CPids,Packet,QoS) ->
    [send_packet(CPid,Packet,QoS) || CPid <- CPids].

send_packets(Range,CPid,#state{filter = Filter, queue = SQ}) ->
    Ps = get_packets(Range,Filter,SQ),
    [send_packet(CPid,P,QoS) || {P,QoS} <- Ps].

send_packet(CPid,P,?QOS_0)   -> mqtt_session_out:push_qos0(CPid,P);
send_packet(CPid,P,QoS)      -> mqtt_session_out:push_reliable(CPid,P,QoS).

get_packets({From,To},_Filter,SQ) when To > From-> ok;
get_packets(_,_,_) -> [].

%% ===================================================================
%% Setting Subscriptions
%% ===================================================================

set_sub({CId,QoS,CSeq,PSeq,Pid},S = #state{monref_idx = Mons,
                                           live_subs = C,
                                           min_acks = MSeq}) ->
    MSeq1 = min_val_tree:store(CId,PSeq,MSeq),
    M = monitor(process, Pid),
    Mons1 = dict:store(M, CId, Mons),
    Reg = #sub{client_seq = CSeq,
               qos = QoS,
               pid = Pid,
               monref = M},
    C1 = dict:store(CId,Reg,C),
    S#state{live_subs = C1,
            monref_idx = Mons1,
            min_acks = MSeq1}.

maybe_remove_downed(MonRef,S = #state{filter = Filter,
                                      live_subs = Subs,
                                      monref_idx = Mon}) ->
    case dict:find(MonRef,Mon) of
        {ok,ClientId} ->
            mqtt_sub_repo:clear_sub_pid(Filter,ClientId),
            Cs1 = dict:update(ClientId,
                fun(Reg) -> Reg#sub{monref = undefined,pid = undefined} end,
                Subs),
            Mon1 = dict:erase(MonRef,Mon),
            S#state{monref_idx = Mon1,live_subs = Cs1};
        error -> S
    end.

recover(S,SubRecord) ->
    lists:foldl(fun set_sub/2,S,SubRecord).

maybe_demonitor_reg(undefined,Mons) ->
    Mons;
maybe_demonitor_reg(Ref,Mons)       ->
    demonitor(Ref,[flush]),
    dict:erase(Ref,Mons).

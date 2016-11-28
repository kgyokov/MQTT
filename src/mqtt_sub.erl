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
    filter          ::binary(), %% Filter handled by this process (e.g. /A/B/+
    live_subs       ::dict(),    %% dictionary of {ClientId::binary(),#client_reg{}}
    packet_seq      ::non_neg_integer(),%% A sequence number assigned to each Message from a topic covered by this filter.
                                    %% This process assigns incremental integers to each new message
    client_seqs     ::min_val_tree:tree(client_id(),fun()),
    queue           ::any(),    %% Shared queue of messages
    retained =      shared_set:new() ::shared_set:set(),   %% Dictionary of retained messages
    garbage         ::any()     %% garbage stats from the queue
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
             Qos::qos(), ResumeFrom::any(), WSize::non_neg_integer())
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
init([Filter, _Repo]) ->
    self() ! async_init,
    mqtt_sub_repo:claim_filter(Filter,self()),
    Seq = 0,
    {ok,#state{filter = Filter,
               live_subs = dict:new(),
               packet_seq = Seq,
               queue = shared_queue:new(Seq),
               retained = shared_set:new()}}.

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


handle_call({sub,ClientId,CSeq,QoS,WSize},{Pid,_},S) ->
    handle_sub(ClientId,CSeq,QoS,WSize,Pid,S);

handle_call({resume,ClientId,CSeq,QoS,ResumeFrom,WSize},{Pid,_},S) ->
    handle_resume({ClientId,CSeq,QoS,ResumeFrom,WSize},Pid,S);

handle_call({unsub,ClientId,Seq}, _From,S) ->
    handle_unsub(ClientId,Seq,S);

handle_call({push,Packet},_From, S) ->
    handle_push(Packet,S);

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
%%% Handling Packets and Acks
%%%===================================================================

-type(order() :: gt | lt | eq).
-type(partial_order() :: order() | undefined).
-type(client_seq():: undefined
                    | non_neg_integer()
                    | complete).

-spec(is_less_than(client_seq(),client_seq()) -> boolean()).
is_less_than(A,B) -> compare(A,B) =:= lt.

-spec(compare(client_seq(),client_seq())-> order()).

compare(A,A)               -> eq;
compare(complete,_)        -> gt;
compare(_,complete)        -> lt;
compare(undefined,_)       -> lt;
compare(_,undefined)       -> gt;
compare(A,B) when is_integer(A),
                  is_integer(B) ->
    case A > B of
        true -> gt;
        _    -> lt
    end.


handle_push(Packet,S = #state{filter = Filter,
                              live_subs = Subs,
                              queue = Q,
                              retained = Ret}) ->
    SQ1 = shared_queue:pushr({Packet#packet.qos,Packet},Q),
    NewSeq = shared_queue:max_seq(SQ1),
    Ret1 = maybe_store_retained(Packet,NewSeq,Ret),
    {SendTo,Subs1} = dict:fold(update_waiting_subs(Q),{[],Subs},Subs),
    S1 = S#state{live_subs = Subs1,
                 queue = SQ1,
                 retained = Ret1},
    send_packets_to_clients(Filter,SendTo),
    {reply,{ok,NewSeq},S1}.

update_waiting_subs(Q) ->
    fun(ClientId,{Pid,Sub},AccIn = {SendToAcc,SubsAcc}) ->
        {Packets,Sub1} = mqtt_sub_state:take_any(Q,Sub),
        case Packets of
            [] ->  AccIn;
            _ ->
                SubsAcc1 = dict:store(ClientId,{Pid,Sub1},SubsAcc),
                SendToAcc1 = [{Pid,Packets}|SendToAcc],
                {SendToAcc1,SubsAcc1}
        end
    end.

handle_pull(WSize,ClientId,S = #state{filter = Filter,
                                      live_subs = Subs,
                                      queue = SQ}) ->
    case dict:find(ClientId,Subs) of
        {ok,{Pid,Sub}} ->
            {Packets,Sub1} = mqtt_sub_state:take(WSize,SQ,Sub),
            S1 = S#state{live_subs = dict:store(ClientId,{Pid,Sub1},Subs)},
            send_packets_to_client(Pid,Filter,Packets),
            S1;
        error -> S
    end.

handle_ack(ClientId,{_RetAck,QAck},S = #state{queue = Q,
                                              garbage = Garbage}) ->
    {MoreGarbage,Q1} = shared_queue:forward(ClientId,QAck,Q),
    S#state{queue = Q1,garbage = ?MONOID:as(Garbage,MoreGarbage)}.

%% handle_ack(_ClientId,{ret,_QAck},S) -> S. %% We don't do anything with ack-ed retained messages


maybe_store_retained(#packet{retain = false},_Seq,Ret)               -> Ret;
maybe_store_retained(#packet{topic = Topic,content = <<>>},Seq,Ret)  -> shared_set:remove(Topic,Seq,Ret);
maybe_store_retained(Packet = #packet{topic = Topic},Seq,Ret)        -> shared_set:append(Topic,Packet,Seq,Ret).

%% ===================================================================
%% Packet Sending
%% ===================================================================

send_packet_to_clients(CPids,Filter,Packet) ->
    [send_packet(CPid,Filter,Packet) || CPid <- CPids].

send_packets_to_clients(Filter,SendTo) ->
    lists:foreach(fun({Pid,Ps}) -> send_packets_to_client(Pid,Filter,Ps) end,SendTo).

send_packets_to_client(CPid,Filter,Packets) ->
    [send_packet(CPid,Filter,P) || P <- Packets].

send_packet(CPid,Filter,P) -> mqtt_session_out:push(CPid,Filter,P).

%% ===================================================================
%% Managing Subscriptions
%% ===================================================================

handle_sub(ClientId,CSeq,QoS,WSize,Pid,S = #state{filter = Filter,
                                                  live_subs = Subs}) ->
    NewSub = {ClientId,CSeq,QoS,Pid},
    case dict:find(ClientId,Subs) of
        error ->
            mqtt_sub_repo:save_sub(Filter,NewSub),
            S1 = insert_new_sub(NewSub,WSize,S),
            {reply,ok,S1};
        {ok,{Pid,Sub}} ->
            case mqtt_sub_state:is_old(CSeq,Sub) of
                true ->
                    %% Message from an old process. Ignore it!
                    %% This can happen in the following (unlikely, but theoretically possible) scenario:
                    %% 1. Process A is spawned for this ClientId and sends a 'sub' message to this process
                    %% 2. Process A dies
                    %% 3. Process B is spawned for the same ClientId and sends another 'sub' message
                    %% 4. 'sub' message from process A is received and this process starts to monitor process A
                    %% 5. Because the 'monitor' call is asynchronous, 'sub' message from process B is received
                    %%      while this process still thinks process A is alive
                    {reply,duplicate,S};
                false ->
                    mqtt_sub_repo:save_sub(Filter,NewSub),
                    S1 = resubscribe(ClientId,QoS,Pid,Sub,S),
                    {reply,ok,S1}
            end
    end.

handle_unsub(ClientId,CSeq, S = #state{filter = Filter,
                                      live_subs = Subs,
                                      queue = SQ,
                                      retained = Ret,
                                      garbage = Garbage}) ->
    case dict:find(ClientId,Subs) of
        error ->
            {reply,ok,S};
        {ok,{_,Sub}} ->
            case mqtt_sub_state:is_old(CSeq,Sub) of
                true ->
                    {reply,ok,S};
                _ ->
                    mqtt_sub_repo:remove_sub(Filter,ClientId),
                    Subs1 = dict:erase(ClientId,Subs),
                    {MoreGarbage,SQ1} = shared_queue:remove(ClientId,SQ),
                    MinSeq = shared_queue:min_seq(SQ1),
                    Ret1 = shared_set:truncate(MinSeq,Ret),
                    S1 = S#state{live_subs = Subs1,
                                 queue = SQ1,
                                 retained = Ret1,
                                 garbage = ?MONOID:as(Garbage,MoreGarbage)},
                    case dict:size(Subs1) of
                        0 -> {stop,no_subs,ok,S1};
                        _ -> {reply,ok,S1}
                    end

            end
    end.

handle_resume({ClientId,CSeq,QoS,ResumeFrom,WSize},Pid,S = #state{filter = Filter,
                                                                  live_subs = Subs,
                                                                  queue = SQ,
                                                                  retained = Ret}) ->
    ShouldResume =
        case dict:find(ClientId,Subs) of
            error -> true;
            {ok,{_,Sub}} -> not mqtt_sub_state:is_old(CSeq,Sub)
        end,
    case ShouldResume of
        true ->
            {Packets,Sub1} = mqtt_sub_state:new(CSeq,QoS,ResumeFrom,WSize,SQ,Ret),
            send_packets_to_client(Pid,Filter,Packets),
            S#state{live_subs = dict:store(ClientId,{Pid,Sub1},Subs)};
        _ -> S
    end.

insert_new_sub({ClientId,CSeq,QoS,Pid},WSize,S = #state{filter = Filter,
                                                        live_subs = Subs,
                                                        queue = Q,
                                                        retained = Ret}) ->
    {Packets,Sub} = mqtt_sub_state:new(Pid,CSeq,QoS,WSize,Q,Ret),
    send_packets_to_client(Pid,Filter,Packets),
    S#state{live_subs = dict:store(ClientId,{Pid,Sub},Subs),
            queue = shared_queue:add(ClientId,Q)}.

%%
%% Only update existing properties - this is not a new subscription
%%
resubscribe(ClientId,QoS,Pid,Sub,S = #state{filter = Filter,
                                            retained = Ret,
                                            queue = Q,
                                            live_subs = Subs}) ->
    {Packets,Sub1} = mqtt_sub_state:resubscribe(QoS,Q,Ret,Sub),
    send_packets_to_client(Pid,Filter,Packets),
    S#state{live_subs = dict:store(ClientId,{Pid,Sub1},Subs)}.

%% @doc
%% We know that a client is subscribed to a queue, we just don't know the index (Seq number)
%% so we default it to 0. The client will "forward" its subscription to the appropriate Seq number once
%% it comes back online
%% @end
recover_sub({ClientId,_QoS,_CSeq,_ClientPid},Q) ->
    shared_queue:add(ClientId,0,Q).

recover(SubRecord,S = #state{queue = Q}) ->
    Q1 = lists:foldl(fun recover_sub/2,Q,SubRecord),
    S#state{queue = Q1}.


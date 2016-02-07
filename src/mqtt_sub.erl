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
-export([start_link/2, subscribe_self/4, unsubscribe/3, get_live_clients/1, new/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SUPERVISOR, mqtt_sub_sup).
-define(REPO, mqtt_sub_repo).

-record(state, {
    repo        ::module(),
    filter      ::binary(),
    clients     ::any(), %% dictionary of {ClientId::binary(),#client_reg{}}
    monref_idx  ::any()  %% dictionary of (MonitorRef,ClientId}
}).

-record(client_sub, {
    qos         ::qos(), %% QoS for this client subscription
    monref      ::any(), %% monitor reference for the process handling the client
    pid         ::pid(), %% Process id of the process handling the client
    client_seq  ::non_neg_integer() %% the version number of the client process registration
                                    %% (incremented every time a new client process is spawned)
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

-spec(subscribe_self(Pid::pid(),ClientId::client_id(),QoS::qos(),Seq::non_neg_integer())
        -> ok).
subscribe_self(Pid,ClientId,QoS,Seq) ->
    gen_server:call(Pid,{sub,ClientId,QoS,Seq}).

-spec(unsubscribe(Pid::pid(),ClientId::client_id(),Seq::non_neg_integer())
        -> ok).
unsubscribe(Pid,ClientId,Seq) ->
    gen_server:call(Pid,{unsub,ClientId,Seq}).

-spec(get_live_clients(Pid::pid()) ->
    [{ClientId::client_id(),QoS::qos(),Pid::pid()}]).
get_live_clients(Pid) ->
    gen_server:call(Pid,get_live_clients).

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
    {ok,#state{filter = Filter, monref_idx = orddict:new(),repo = Repo}}.

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


handle_call({sub,ClientId,NewQoS,NewSeq},{NewPid,_},S = #state{filter = Filter,
                                                               clients = Clients,
                                                               monref_idx = Mon}) ->
    NewSub = {ClientId,NewQoS,NewSeq,NewPid},
    CurrentClient = orddict:find(ClientId,Clients),
    case CurrentClient of
        error ->
            mqtt_sub_repo:save_sub(Filter,NewSub),
            S1 = set_sub(S,NewSub),
            {reply,ok,S1};
        {ok,CurReg} ->
            case CurReg of
                #client_sub{pid = OldPid, client_seq = CurSeq} when CurSeq > NewSeq ->
                    Resp = if OldPid =:= NewPid; OldPid =:= undefined ->
                                        ok; %%ignore old messages from this Pid
                                true -> duplicate %% this is an old process
                            end,
                    {reply,Resp,S};
                #client_sub{pid = NewPid,qos = NewQoS} ->
                    {reply,ok,S}; %% nothing to change
                #client_sub{pid = NewPid} ->
                    mqtt_sub_repo:save_sub(Filter,NewSub),
                    %% replace existing QoS and Seq
                    S1 = S#state{clients = orddict:store(
                        ClientId,
                        CurReg#client_sub{qos = NewQoS, client_seq = NewSeq},
                        Clients)},
                    {reply,ok,S1};
                #client_sub{monref = OldRef, pid = OldPid} when OldPid =/= NewPid ->
                    mqtt_sub_repo:save_sub(Filter,NewSub),
                    %% replace existing Pid
                    S1 = S#state{monref_idx = maybe_demonitor_reg(OldRef,Mon)},
                    S2 = set_sub(S1,NewSub),
                    {reply,ok,S2}
            end
    end;

handle_call({unsub,ClientId,NewSeq}, _From, S = #state{clients = Clients,
                                                       monref_idx = Mon}) ->
    CurrentClient = orddict:find(ClientId,Clients),
    case CurrentClient of
        error ->
            {reply,ok,S};
        {ok,ExistingReg} ->
            case ExistingReg of
               #client_sub{monref = MonRef, client_seq = Seq} when NewSeq >= Seq ->
                   mqtt_sub_repo:remove_sub(S#state.filter,ClientId),
                   Mon1 = maybe_demonitor_reg(MonRef,Mon),
                   Clients1 = orddict:erase(ClientId,Clients),
                   S1 = S#state{monref_idx = Mon1,clients = Clients1},
                   case orddict:size(Clients1) of
                       0 -> {stop,no_clients,ok,S1};
                       _ -> {reply,ok,S1}
                   end;
                _ ->
                    {reply,ok,S}
            end
    end;

handle_call(get_live_clients, _From, S = #state{clients = Clients}) ->
    ClientPids = [
        {ClientId,QoS,Pid} ||
        {ClientId,#client_sub{pid = Pid,qos = QoS}} <- orddict:to_list(Clients), Pid =/= undefined],
    {reply, ClientPids, S};

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

handle_info({'DOWN', MonRef, _, _, _}, S = #state{filter = Filter,
                                                  clients = Clients,
                                                  monref_idx = Mon}) ->
    S1  =
        case orddict:find(MonRef,Mon) of
            {ok,ClientId} ->
                mqtt_sub_repo:clear_sub_pid(Filter,ClientId),
                Clients1 = orddict:update(ClientId,
                                fun(Reg) ->
                                    Reg#client_sub{monref = undefined,
                                                   pid = undefined}
                                end,
                                Clients),
                Mon1 = orddict:erase(MonRef,Mon),
                S#state{monref_idx = Mon1,clients = Clients1};
            error -> S
        end,
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


set_sub(S = #state{monref_idx = Mon,clients = Clients},{ClientId,NewQoS,NewSeq,NewPid}) ->
    MonRef = monitor(process,NewPid),
    Mon1 = orddict:store(MonRef,ClientId,Mon),
    NewReg = #client_sub{client_seq = NewSeq,
                         qos = NewQoS,
                         pid = NewPid,
                         monref = MonRef},
    Clients1 = orddict:store(ClientId,NewReg,Clients),
    S#state{clients = Clients1, monref_idx = Mon1}.

%% remove_sub(S = #state{monref_idx = Mon,clients = Clients},ClientId) ->
%%     case orddict:find(ClientId,Clients) of
%%         error ->
%%             S;
%%         {ok,#client_reg{monref = MonRef}} ->
%%             Clients1 = orddict:erase(ClientId,Clients),
%%             Mon1 = case MonRef of
%%                         undefined -> Mon;
%%                         _ ->
%%                             demonitor(MonRef,[flush]),
%%                             orddict:erase(MonRef,Mon)
%%                    end,
%%             S#state{clients = Clients1,monref_idx = Mon1}
%%     end.
%%
%% process_down(S = #state{monref_idx = Mon,clients = Clients},MonRef) ->
%%     case orddict:find(MonRef,Mon) of
%%         error -> S;
%%         {ok,ClientId} ->
%%             Mon1 = orddict:erase(MonRef,Mon),
%%             Clients1 = orddict:update(ClientId,
%%                                         fun(Reg) -> Reg#client_reg{pid = undefined,monref = undefined} end,
%%                                     Clients),
%%             S#state{monref_idx = Mon1,clients = Clients1}
%%     end.
%%
%%
%% replace_sub(S = #state{monref_idx = Mon,clients = Clients},{ClientId,NewQoS,NewSeq},NewPid) ->
%%     ok.


recover(S,SubRecord) ->
    lists:foldr(fun set_sub/2,S,SubRecord).


maybe_demonitor_reg(undefined,Mons) ->
    Mons;
maybe_demonitor_reg(Ref,Mons)       ->
    demonitor(Ref,[flush]),
    orddict:erase(Ref,Mons).


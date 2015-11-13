%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Nov 2015 10:05 PM
%%%-------------------------------------------------------------------
-module(mqtt_sub).
-author("Kalin").

-include("mqtt_internal_msgs.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2, subscribe/4, unsubscribe/3, get_live_clients/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    repo        ::module(),
    filter      ::binary(),
    clients     ::any(), %% dictionary of {ClientId::binary(),#client_reg{}}
    monitored   ::any()  %% dictionary of (MonitorRef,ClientId}
}).

-record(client_reg, {
    qos         ::qos(),
    monref      ::any(),
    pid         ::pid(),
    seq         ::non_neg_integer()
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
-spec(start_link(Filter::binary(),Mod::module()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Filter,Mod) ->
    gen_server:start_link(?MODULE, [Filter,Mod], []).

-spec(subscribe(Pid::pid(),ClientId::client_id(),QoS::qos(),Seq::non_neg_integer())
        -> ok).
subscribe(Pid,ClientId,QoS,Seq) ->
    gen_server:call(Pid,{sub,ClientId,QoS,Seq}).

unsubscribe(Pid,ClientId,Seq) ->
    gen_server:call(Pid,{unsub,ClientId,Seq}).

get_live_clients(Pid) ->
    gen_server:call(Pid,get_live_clients).

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
    {ok,#state{filter = Filter,monitored = orddict:new(),repo = Repo}}.

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


handle_call({sub,ClientId,NewQoS,NewSeq},{NewPid,_},S = #state{clients = Clients,
                                                               monitored = Mon}) ->
    CurrentClient = orddict:find(ClientId,Clients),
    case CurrentClient of
        error ->
            {NewRef,Mon1} = monitor_reg(Clients,ClientId,NewPid),
            NewVal = #client_reg{seq = NewSeq,
                                 qos = NewQoS,
                                 pid = NewPid,
                                 monref = NewRef},
            Clients1 = orddict:update(ClientId,NewVal,Clients),
            NewState = S#state{monitored = Mon1,
                               clients = Clients1},
            {reply,ok,NewState};
        {ok,CurReg} ->
            case CurReg of
                #client_reg{pid = OldPid, seq = CurSeq} when CurSeq > NewSeq ->
                    if OldPid =:= NewPid -> {reply,ok,S}; %%ignore old messages from this Pid
                        true ->             {reply,duplicate,S}
                    end;
                #client_reg{pid = NewPid,qos = NewQoS} ->
                    {reply,ok,S}; %% nothing to change
                #client_reg{pid = NewPid} ->
                    %% replace existing QoS
                    NewReg = CurReg#client_reg{qos = NewQoS,seq = NewSeq},
                    NewState = S#state{clients = orddict:update(ClientId,NewReg,Clients)},
                    {reply,ok,NewState};
                #client_reg{monref = OldRef, pid = OldPid} when OldPid =/= NewPid ->
                    %% replace existing Pid
                    {_,Mon1} = demonitor_reg(Mon,OldRef),
                    {NewRef,Mon2} = monitor_reg(Mon1,ClientId,NewPid),
                    NewReg = #client_reg{seq = NewSeq,
                                         qos = NewQoS,
                                         pid = NewPid,
                                         monref = NewRef},
                    NewClients = orddict:update(ClientId,NewReg,Clients),
                    NewState = S#state{clients = NewClients,
                                       monitored = Mon2},
                    {reply,ok,NewState}
            end
    end;

handle_call({unsub,ClientId,NewSeq}, _From, S = #state{clients = Clients,
                                                        monitored = Mon}) ->
    CurrentClient = orddict:find(ClientId,Clients),
    case CurrentClient of
        error ->
            {reply,ok,S};
        {ok,ExistingReg} ->
            case ExistingReg of
               #client_reg{monref = MonRef, seq = Seq} when NewSeq >= Seq ->
                   {_,Mon1} = demonitor_reg(Mon,MonRef),
                   Clients1 = orddict:erase(ClientId,Clients),
                   {reply,ok,S#state{monitored = Mon1,clients = Clients1}};
                _ ->
                    {reply,ok,S}
            end
    end;

handle_call(get_live_clients, _From, S = #state{clients = Clients}) ->
    ClientPids = [
        {ClientId,QoS,Pid} ||
        {ClientId,#client_reg{pid = Pid,qos = QoS}} <- orddict:to_list(Clients), Pid =/= undefined],
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

handle_info({'DOWN', MonitorRef, _, _, _}, S = #state{clients = Clients,
                                                      monitored = Mon}) ->
    {ClientId,Mon1} = demonitor_reg(Mon,MonitorRef),
    orddict:update(ClientId,
        fun(Reg) -> Reg#client_reg{monref = undefined,
                                   pid = undefined}
        end,Mon),
    Clients1 = orddict:erase(ClientId,Clients),
    {noreply, S#state{monitored = Mon1,clients = Clients1}};

handle_info(async_init, S = #state{filter = Filter,repo = Repo}) ->
    Subs = Repo:load(Filter),
    S1 = load_sub(S,Subs),
    {noreply,S1};
%%     Clients = [{ClientId,#client_reg{qos = QoS,seq = Seq}} || {ClientId,QoS,Seq} <- Subs],
%%     {noreply, S#state{clients = Clients,monitored = orddict:new()}};

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
terminate(_Reason, #state{filter = Filter,repo = Repo}) ->
    repo = Repo:clear_pid(Filter).

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


%% handle_sub(ClientId,NewQoS,NewSeq,NewPid,S = #state{clients = Clients,
%%                                                     monitored = Mon}) ->
%%     ok.

load_sub(S,SubRecord) ->
    Clients = [{ClientId,#client_reg{qos = QoS,seq = Seq}} || {ClientId,QoS,Seq} <- SubRecord],
    S#state{clients = Clients,monitored = orddict:new()}.

monitor_reg(Mons,ClientId,Pid) ->
    Ref = monitor(process,Pid),
    {Ref,orddict:store(Ref,ClientId,Mons)}.

demonitor_reg(Mons,Ref) ->
    demonitor(Ref,[flush]),
    {ok,ClientId} = orddict:find(Ref,Mons),
    {ClientId,orddict:erase(Ref)}.


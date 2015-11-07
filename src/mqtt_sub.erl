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
-export([start_link/1, subscribe/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    filter      ::binary(),
    clients     ::any(), %% dictionary of {ClientId::binary(),#client_reg{}}
    monitored   ::any()  %% dictionary of (MonitorRef,ClientId}
}).

-record(client_reg, {
    qos         ::qos(),
    monitored   ::ref(),
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
-spec(start_link(Filter::binary()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Filter) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Filter], []).

-spec(subscribe(Pid::pid(),ClientId::client_id(),QoS::qos())
        -> ok).
subscribe(Pid,ClientId,QoS) ->
    gen_server:call(Pid,{sub,ClientId,QoS}).

unsubscribe(Pid,ClientId) ->
    gen_server:call(Pid,{unsub,ClientId}).

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
init([Filter]) ->
    {ok, #state{filter = Filter,
                clients = orddict:new(),
                monitored = orddict:new()}}.

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


handle_call({sub,ClientId,NewQoS,NewSeq},{FromPid,_},S = #state{clients = Clients,
                                                                monitored = Mon}) ->
    CurrentClient = orddict:find(ClientId,Clients),
    case CurrentClient of
        error ->
            {NewRef,Mon1} = monitor_reg(Clients,ClientId,FromPid),
            Clients1 = orddict:update(ClientId,{NewQoS,NewRef,FromPid,NewSeq},Clients),
            NewState = S#state{monitored = Mon1,
                                clients = Clients1},
            {reply,ok,NewState};
        {ok,Record} ->
            case Record of
                {_,_,FromPid,CurSeq} when CurSeq > NewSeq ->
                    {reply,ok,S}; %%ignore old messages
                {_,_,_OtherPid,CurSeq} when CurSeq > NewSeq ->
                    {reply,duplicate,S}; %%ignore old messages
                {NewQoS,_,FromPid,_} ->
                    {reply,ok,S}; %% nothing to change
                {_,_,FromPid,_} ->
                    %% replace existing QoS
                    NewVal = {NewQoS,FromPid,NewSeq},
                    NewState = S#state{clients = orddict:update(ClientId,NewVal,Clients)},
                    {reply,ok,NewState};
                {_,OldRef,_,_} ->
                    %% replace existing Pid
                    {_,Mon1} = demonitor_reg(Mon,OldRef),
                    {NewRef,Mon2} = monitor_reg(Mon1,ClientId,FromPid),
                    NewClients = orddict:update(ClientId,{NewQoS,NewRef,FromPid,NewSeq},Clients),
                    NewState = S#state{clients = NewClients,
                                        monitored = Mon2},
                    {reply,ok,NewState}
            end

    end,
    NewState = ok,
    {reply,ok,NewState};

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



monitor_reg(Regs,ClientId,Pid) ->
    Ref = monitor(process,Pid),
    {Ref,orddict:store(Pid,{ClientId,Ref},Regs)}.

demonitor_reg(Regs,Pid) ->
    {ok,{ClientId,Ref}} = orddict:find(Pid,Regs),
    demonitor(Ref),
    {ClientId,orddict:erase(Ref)}.


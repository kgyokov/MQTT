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
-export([start_link/2, push_qos0/2, push_reliable/3, new/2]).

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
    conn_pid,
    session_out
}).

%%%===================================================================
%%% API
%%%===================================================================


new(SupPid,CleanSession) ->
    {ok,SessionPid} = mqtt_connection_sup2:create_session(SupPid,self(),CleanSession),
    SessionPid.
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(ConnPid::pid(),CleanSession::boolean()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(ConnPid,CleanSession) ->
    gen_server:start_link(?MODULE, [ConnPid,undefined,CleanSession], []).

push_qos0(Pid, CTRPacket) ->
    gen_server:cast(Pid,{push_0, CTRPacket}).

push_reliable(Pid, CTRPacket,QoS) ->
    gen_server:call(Pid,{push_reliable,CTRPacket,QoS}).

push_reliable_comp(Pid, CTRPacket,QoS) ->
    gen_server:call(Pid,{push_reliable_comp,CTRPacket,QoS}).

%% append_msg(Pid,CTRPacket = {_Topic,_Content,_Retain,_QoS},Ref) ->
%%     gen_server:call(Pid,{append, CTRPacket,Ref}).

%% append_message_comp(Pid,Ref) ->
%%     gen_server:call(Pid,{append_comp,Ref}).

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
    self() ! async_init,
    {ok, #state{conn_pid = ConnPid, session_out = mqtt_session:new(ClientId,CleanSession)}}.

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

handle_call({push_reliable, CTRPacket,QoS}, _From, S = #state{session_out = SO,conn_pid = ConnPid})
    when QoS =:= 1; QoS =:= 2 ->
    error_logger:info_msg("Received packet ~p with QoS = ~p~n",[CTRPacket,QoS]),
    case mqtt_session:append_msg(SO,CTRPacket,QoS) of
          duplicate ->
              {reply,duplicate,S#state{session_out = SO}};
        {ok, SO1} ->
            mqtt_connection:publish_packet(ConnPid,CTRPacket,QoS),
            {reply,ok,SO1}
     end;

handle_call({append_comp,Ref}, _From,  S = #state{session_out = SO}) ->
    SO1 = mqtt_session:append_message_comp(SO,Ref),
    {reply,ok,S#state{session_out = SO1}};

handle_call({ack,PacketId}, _From,  S = #state{session_out = SO}) ->
    SO1 = mqtt_session:message_ack(SO,PacketId),
    {reply,ok,S#state{session_out = SO1}};

handle_call({pub_rec,PacketId}, _From,  S = #state{session_out = SO}) ->
    SO1 = mqtt_session:message_pub_rec(SO,PacketId),
    {reply,ok,S#state{session_out = SO1}};

handle_call({pub_comp,PacketId}, _From,  S = #state{session_out = SO}) ->
    SO1 = mqtt_session:message_pub_comp(SO,PacketId),
    {reply,ok,S#state{session_out = SO1}};

handle_call({sub,NewSub = {_,Qos}}, _From,  S = #state{session_out = SO}) ->
    SO1 = mqtt_session:subscribe(SO,[NewSub]),
    {reply,{ok,Qos},S#state{session_out = SO1}};

handle_call({unsub,OldSubs}, _From,  S = #state{session_out = SO}) ->
    SO1 = mqtt_session:unsubscribe(SO,OldSubs),
    {reply,ok,S#state{session_out = SO1}};

%% handle_call(cleanup, _From, S = #state{session_out = SO}) ->
%%     SO1 = mqtt_session:cleanup(SO),
%%     {reply,ok,S#state{session_out = SO1}};

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

handle_cast({push_qos0,CTRPacket}, S = #state{conn_pid = ConnPid}) ->
    mqtt_connection:publish_packet(ConnPid,CTRPacket,0),
    {noreply,S};

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


handle_info({async_init,ClientId}, #state{}) ->
    mqtt_reg_repo:register(ClientId);

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
terminate(_Reason, #state{session_out = SO}) ->
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

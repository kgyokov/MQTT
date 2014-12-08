%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2014 6:14 PM
%%%-------------------------------------------------------------------
-module(mqtt_server).
-author("Kalin").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/2,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).



-record(state, {
  connect_state = starting, %% CONNECT state: starting, connected, disconnecting, disconnected
  sender_pid,                 %% The process communicating with the actual device
  options,                  %% options such as connection timeouts, etc.
  connect_details,           %% client details, e.g. client id,
  session = #{},
  keep_alive_ref = undefined, %% so we can ignore old keep-alive timeout messages after restarting the timer
  keep_alive_timeout = 0
}).

-record(connect_details, {
  client_id,
  qos
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
%% -spec(start_link() ->
%%   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link(?MODULE, [], []).

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
%% -spec(init(Args :: term()) ->
%%   {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
%%   {stop, Reason :: term()} | ignore).
init(SenderPid,Options) ->
  process_flag(trap_exit,true),
  {ok, #state{
    connect_state = starting,
    sender_pid =  SenderPid,
    options = Options }}.

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


handle_call({packet, PacketType, Details}, From, S) ->
  S1 = maybe_reset_keep_alive(S), %% shared functionality between all packets
  handle_packet({PacketType, Details}, From, S1);

%% handle_call(keep_alive_timeout, From, S#state{sender_pid = CommPid})->
%%   gen_server:cast(CommPid, {disconnect, keep_alive_timeout}),
%%   {stop, normal, S#state{connect_state = disconnecting}}
%% ;

handle_call({malformed_packet}, From, S) ->
  gen_server:reply(From, {disconnect, malformed_packet}),
  {stop,normal, S#state{connect_state = disconnecting}};

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

handle_info({keep_alive_timeout,Ref}, S#state{ keep_alive_ref = Ref, sender_pid = SenderPid})->
  gen_server:cast(SenderPid, {disconnect, keep_alive_timeout}),
  {stop, normal, S#state{connect_state = disconnecting}};

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

handle_packet({'CONNECT', Details = {KeepAlive}}, From, S#state{connect_state = starting}) ->
  %% TODO: validate

  start_keep_alive(S),
  gen_server:reply(From,{send, 'CONNACK'}),

  {noreply,
    S#state{
      connect_state = connected,
      connect_details = Details
    }
  };

handle_packet({'CONNECT', From}, _, S) ->
  gen_server:reply(From, {disconnect, duplicate_connect}),
  {stop,
    duplicate_connect,
    S#state{connect_state = disconnecting}};

handle_packet({'PUBLISH', _}, _, S) ->
  0;

handle_packet({'PUBACK', _}, _, S) ->
  0;

handle_packet({'PUBREC', _}, _, S) ->
  0;

handle_packet({'PUBREL', _}, _, S) ->
  0;

handle_packet({'PUBCOMP', _}, _, S) ->
  0;

handle_packet({'SUBSCRIBE', _}, _, S) ->
  0;

handle_packet({'UNSUBSCRIBE', _}, _, S) ->
  0;

handle_packet({'PINGREQ', _}, From, S) ->
  gen_server:reply(From, {send, 'PINGRESP'});

handle_packet({'DISCONNECT', _}, From, S) ->
  gen_server:reply(From, {disconnect, normal}),
  {stop,
    duplicate_connect,
    {disconnect, normal},
    S#state{connect_state = disconnecting}};

handle_packet({'Reserved', _}, _, S) ->
  0.

start_keep_alive(S)->
  if S#state.keep_alive_timeout > 0 ->
    S#state{keep_alive_ref = timer:send_after(S#state.keep_alive_timeout, keep_alive_timeout)};
    true -> S
  end
.

maybe_reset_keep_alive(S)->
  if S#state.keep_alive_ref =/= undefned ->
      timer:cancel(S#state.keep_alive_ref);
    true -> ok
  end,
  start_keep_alive(S)
.

send(To,Message)->
  gen_server:reply(To, Message)
.


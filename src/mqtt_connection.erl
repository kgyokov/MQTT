%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2014 6:14 PM
%%%-------------------------------------------------------------------
-module(mqtt_connection).
-author("Kalin").

-include("mqtt_packets.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0,
  process_packet/2,
  process_client_disconnect/2,
  process_malformed_packet/2,
  close_duplicate/1]).

%% gen_server callbacks
-export([init/2,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).



-record(state, {
  connect_state = connecting, %% CONNECT state: connecting, connected, disconnecting, disconnected
  sender_pid,                 %% The process sending to the actual device
  receiver_pid,               %% The process receiving from the actual device
  options,                  %% options such as connection timeouts, etc.
  connect_details,           %% client details, e.g. client id,
  session = #{},
  keep_alive_ref = undefined, %% so we can ignore old keep-alive timeout messages after restarting the timer
  keep_alive_timeout = undefined
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

process_packet(Pid,Packet)->
  gen_server:call(Pid,{packet, Packet}).

process_malformed_packet(Pid,_Reason)->
  gen_server:call(Pid,{malformed_packet}).

process_client_disconnect(Pid,Reason)->
  gen_server:call(Pid,{client_disconnected, Reason}).

close_duplicate(Pid)->
  gen_server:cast(Pid, {force_close, duplicate}).

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


handle_call({packet, Packet}, From, S) ->
  %% shared functionality between all packets
  S1 = case S#state.keep_alive_timeout of
      undefined
        -> S;
      _
        -> S#state{keep_alive_ref = reset_keep_alive(S#state.keep_alive_timeout,S#state.keep_alive_ref)}
     end,
  handle_packet(Packet, From, S1);

%% handle_call(keep_alive_timeout, From, S#state{sender_pid = CommPid})->
%%   gen_server:cast(CommPid, {disconnect, keep_alive_timeout}),
%%   {stop, normal, S#state{connect_state = disconnecting}}
%% ;

handle_call({malformed_packet}, From, S) ->
  disconnect_client(From, malformed_packet),
  {stop,normal, S#state{connect_state = disconnecting}};

handle_call({client_disconnected, _Reason}, From, S) ->
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
handle_cast({force_close,Reason}, State) ->
  {stop,  Reason, State#state{connect_state = disconnecting}};

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

handle_info({keep_alive_timeout,Ref}, S = #state{ keep_alive_ref = Ref, sender_pid = SenderPid})->
  disconnect_client(SenderPid,keep_alive_timeout),
  %gen_server:cast(SenderPid, {disconnect, keep_alive_timeout}),
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

handle_packet(Packet = #'CONNECT' {client_id = ClientId},
    From,
    S = #state{connect_state = connecting} %%disallow duplicate CONNECT packets
) ->
  %% TODO: validate connect packet

  register_self(ClientId),
  S1 = start_keep_alive(S),

  send_to_client(ClientId, #'CONNACK'{flags = undefined, return_code = 0}),

  {noreply,
    S#state{
      connect_state = connected,
      connect_details = Packet
    }
  };

%% Catch all-case
handle_packet(Packet, _From, S = #state{ connect_state = connecting})
  when not is_record(Packet, 'CONNECT')
  ->
  disconnect_client(S#state.sender_pid, 'CONNECT_expected'),
  {stop,
    'CONNECT_expected',
    S#state{connect_state = disconnecting}};

handle_packet(#'CONNECT'{},  _, S) when S#state.connect_state =/= connecting ->
  disconnect_client(S#state.sender_pid, duplicate_CONNECT),
  {stop,
    duplicate_CONNECT,
    S#state{connect_state = disconnecting}};

handle_packet(#'PUBLISH'{}, _, S) ->
  0;

handle_packet(#'PUBACK'{}, _, S) ->
  0;

handle_packet(#'PUBREC'{}, _, S) ->
  0;

handle_packet(#'PUBREL'{}, _, S) ->
  0;

handle_packet(#'PUBCOMP'{}, _, S) ->
  0;

handle_packet(#'SUBSCRIBE'{}, _, S) ->
  0;

handle_packet(#'UNSUBSCRIBE'{}, _, S) ->
  0;

handle_packet(#'PINGREQ'{}, From, S) ->
  send_to_client(From, {'PINGRESP'});

handle_packet(#'DISCONNECT'{}, From, S) ->
  disconnect_client(From, client_request),
  {stop,
    client_request,
    {disconnect, normal},
    S#state{connect_state = disconnecting}};

handle_packet({'Reserved', _}, _, S) ->
  0.

start_keep_alive(Timeout)->
    timer:send_after(Timeout, keep_alive_timeout)
.

reset_keep_alive(Ref,TimeOut)->
  if Ref =/= undefned ->
      timer:cancel(TimeOut);
    true -> ok
  end,
  start_keep_alive(TimeOut)
.

send_to_client(ClientPid,Packet)->
  gen_server:call(ClientPid, {send, Packet}).

disconnect_client(ClientPid,Reason)->
  gen_server:call(ClientPid,{disconnect,Reason}).

register_self(ClientId)->
  mqtt_registration_repo:register(self(),ClientId).
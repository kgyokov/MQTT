%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Dec 2014 1:29 AM
%%%-------------------------------------------------------------------
-module(mqtt_sender).
-author("Kalin").

-behaviour(gen_server).

%% API
-export([start_link/0,
  send_packet/2,
  send_packet_async/2
%%   disconnect/2,
%%   disconnect_async/2,
%%   connect/4
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
  is_connected = false,
  socket
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
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link(?MODULE, [], []).


send_packet(Pid,Packet)->
  gen_server:call(Pid, {packet,Packet}).

send_packet_async(Pid,Packet)->
  gen_server:cast(Pid, {packet,Packet}).

%% connect(Pid,Host,Port,Options)->
%%   gen_server:call(Pid,{connect,Host,Port,Options}).
%%
%% disconnect(Pid,Reason)->
%%   gen_server:call(Pid,{disconnect,Reason}).
%%
%% disconnect_async(Pid,Reason)->
%%   gen_server:cast(Pid,{disconnect,Reason}).

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
init([]) ->
  {ok, #state{}}.

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

%% handle_call({connect,Host,Port,Options},From,State)->
%%   AllOptions = [{active,false}, {binary,{packet,0}} | Options],
%%   {ok,Socket} = gen_tcp:connect(Host,Port,AllOptions),
%%   NewState = State#state{socket = Socket, is_connected = true},
%%   {reply,ok,NewState}
%%   ;
%%
%% handle_call({disconnect,_Reason},_From,State = #state{is_connected = false})->
%%   {reply,ok, State};
%%
%% handle_call({disconnect,_Reason},_From,State#state{socket = Socket})->
%%   ok = gen_tcp:close(Socket),
%%   {reply,ok,State};



handle_call({packet,_Packet},_From,State = #state{is_connected = false})->
  {reply,{error,not_connected},State};
handle_call({packet,Packet},_From,#state{socket = Socket})->
  mqtt_builder:build_packet(Packet),
  gen_tcp:send(Socket,Packet)
;


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

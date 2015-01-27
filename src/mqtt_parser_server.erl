%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jan 2015 10:13 PM
%%%-------------------------------------------------------------------
-module(mqtt_parser_server).
-author("Kalin").

-include("mqtt_const.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0, disconnect/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


-record(state, {
  socket,
  transport,
  ref,
  opts,
  connection,
  parser
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


disconnect(Pid)->
  gen_server:cast(Pid,disconnect).

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
init([ConnectionPid,Ref,Socket,Transport]) ->
  %% process_flag(trap_exit,true),
  ParserPid = spawn_link(fun() -> loop_over_socket(ConnectionPid,Ref,Socket,Transport) end),
  S = #state{parser = ParserPid},
  {ok, S}.

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

handle_cast(disconnect, State = #state{parser = ParserPid}) ->
  {stop, disconnect, State};

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

handle_info({'EXIT',Reason, ParserPid}, State = #state{parser = ParserPid, connection = ConnectionPid}) ->
  %% mqtt_connection:process_client_disconnect(ConnectionPid, Reason)
  {noreply, State};

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


loop_over_socket(ConnectionPid,Ref,Socket,Transport)->
  %% ranch:accept_ack(Ref),

  %% calback for the parser process to get new data
  ReadFun =
    fun(ExpectedData) ->
      receive_data(Transport,Socket,ExpectedData,5000)
    end,

  ParseState = #parse_state{
    buffer = <<>>,
    max_buffer_size = 100000,
    readfun =  ReadFun
  },
  loop_over_socket(ConnectionPid,ParseState)
.

loop_over_socket(ConnectionPid, ParseState)->
  try mqtt_parser:parse_packet(ParseState) of
    {NewPacket,NewParseState} ->
      mqtt_connection:process_packet(ConnectionPid,NewPacket),
      loop_over_socket(ConnectionPid,NewParseState);
    _
      -> mqtt_connection:process_bad_packet(ConnectionPid,unknown)
  catch
    throw:{error,Reason} ->
      handle_error(ConnectionPid,Reason)

  end
.

handle_error(ConnectionPid, Reason)->
  case Reason of
    invalid_flags ->
      mqtt_connection:process_bad_packet(ConnectionPid,invalid_flags);
    malformed_pdacket ->
      mqtt_connection:process_bad_packet(ConnectionPid,unknown);
    unexpected_disconnect ->
      mqtt_connection:process_unexpected_disconnect(ConnectionPid,unexpected_disconnect)
   %% TODO: More errors
  end
 .

%% callback for parser process
receive_data(Transport,Socket,ExpectedData,TimeOut)->
  %% we can just return the {ok,Data} or {error,_} values directly to the parser process
  Transport:recv(Socket, ExpectedData, TimeOut)
.



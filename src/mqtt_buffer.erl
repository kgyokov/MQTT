%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Mar 2016 2:46 AM
%%%-------------------------------------------------------------------
-module(mqtt_buffer).
-author("Kalin").

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    queue       :: queue:queue(),
    window_size ::non_neg_integer(),
    send_to     ::pid()
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
-spec(start_link(WindowSize::non_neg_integer(),SendTo::pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(WindowSize,SendTo) ->
    gen_server:start_link(?MODULE, [WindowSize,SendTo], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([WindowSize,SendTo]) ->
    {ok, #state{queue = queue:new(),
                window_size = WindowSize,
                send_to = SendTo}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({push,Packet}, S) ->
    handle_push(Packet,S);

handle_cast({pull,From}, S = #state{send_to = From}) ->
    handle_pull(S);

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


handle_push(Msg,S = #state{queue = Q,window_size = WSize}) when WSize =<0 ->
    S#state{queue = queue:cons(Msg,Q)};

handle_push(Msg,S = #state{window_size = WSize}) ->
    send(Msg,S),
    S#state{window_size = WSize - 1}.

send(Packet,#state{send_to = Pid}) ->
    Pid ! {packet,self(),Packet}.

handle_pull(S = #state{queue = Q,window_size = WSize}) ->
    case queue:is_empty(Q) of
        true -> S#state{window_size = WSize+1};
        false ->
            S1 = S#state{queue = queue:tail(Q)},
            Msg = queue:head(Q),
            send(Msg,S1),
            S1
    end.
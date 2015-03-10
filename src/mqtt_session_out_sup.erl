%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Feb 2015 12:12 AM
%%%-------------------------------------------------------------------
-module(mqtt_session_out_sup).
-author("Kalin").

-behaviour(supervisor).

-define(SESSION_SPEC(ConnPid),
    {
        session_serv,
        {mqtt_session_serv, start_link, [ConnPid]},
        permanent,
        200,
        worker,
        [mqtt_session_serv]
    }).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(ConnPid :: pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(ConnPid) ->
    supervisor:start_link(?MODULE, [ConnPid]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init([ConnPid]) ->
    {ok, {{one_for_one, 20, 200}, [?SESSION_SPEC(ConnPid)]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

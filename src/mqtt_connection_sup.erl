%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Jan 2015 1:37 AM
%%%-------------------------------------------------------------------
-module(mqtt_connection_sup).
-author("Kalin").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
%% -spec(start_link() ->
%%   {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  SupPid = supervisor:start_link(?MODULE, []),
  {ok, ReceiverPid } = supervisor:start_child(SupPid,[]),
  {ok, SenderPid } = supervisor:start_child(SupPid,[]),

  ConnArgs = [ReceiverPid,SenderPid,[]],
  {ok, _ConnectionPid } = supervisor:start_child(SupPid,{
    connection,
    {mqtt_connection, start_link, ConnArgs},
    permanent,          % must never stop
    2000,               % should be more than sufficient
    worker,             % as opposed to supervisor
    [mqtt_connection]
  })
.

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
init([]) ->
  RestartStrategy = one_for_all,
  MaxRestarts = 0,
  MaxSecondsBetweenRestarts = 1,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

%%   Restart = permanent,
%%   Shutdown = 2000,
%%   Type = worker,

%%   ConnectionChild = {
%%     connection,
%%     {mqtt_connection, start_link, []},
%%     Restart, Shutdown, Type,
%%     [mqtt_connection]
%%   },

  %%{ok, {SupFlags, [ConnectionChild ]}}.

  {ok, {{one_for_all, 0, 1}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

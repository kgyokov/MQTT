%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Mar 2015 1:40 AM
%%%-------------------------------------------------------------------
-module(mqtt_connection_sup_sup).
-author("Kalin").

-behaviour(supervisor).

%% API
-export([start_link/0, start_link_tree/3]).

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
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    error_logger:info_msg("Start_link ~p",[?SERVER]),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

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
    SupFlags = {simple_one_for_one, 0, 1},

    AChild = {conn_sup,
        {mqtt_connection_sup2, start_link, []},
        temporary, infinity, supervisor, [mqtt_connection_sup2]},

    {ok, {SupFlags, [AChild]}}.

start_link_tree(Transport,Socket,Options) ->
    error_logger:info_msg("Starting Sup",[?SERVER]),
    {ok,SupPid} = supervisor:start_child(?SERVER, []),
    error_logger:info_msg("Started Sup ~p",[SupPid]),
    {ok, _ConnPid} = mqtt_connection_sup2:create_tree(SupPid,self(),Transport,Socket,Options).

%%%===================================================================
%%% Internal functions
%%%===================================================================

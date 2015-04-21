%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Feb 2015 1:05 AM
%%%-------------------------------------------------------------------
-module(mqtt_connection_root_sup).
-author("Kalin").

-behaviour(supervisor).

%% API
-export([start_link/2, handle_socket/3]).

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
-spec(start_link(Options::any(),Security::any()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Options,Security) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Options,Security]).

handle_socket(Ref,Socket,Transport) ->
    supervisor:start_child({local, ?SERVER},[Ref,Socket,Transport,self()]).

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
init([Options,Security]) ->
    {ok, { {simple_one_for_one, 0, 1},
        [
            {mqtt_connection_sup, {mqtt_connection_sup, start_link, [Options,Security]},
                temporary, brutal_kill, supervisor, [mqtt_connection_sup]}
        ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

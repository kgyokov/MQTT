%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jan 2015 11:54 PM
%%%-------------------------------------------------------------------
-module(mqtt_connection_sup2).
-author("Kalin").

-behaviour(supervisor).

-define(SENDER_SPEC(Transport,Socket),
    {
        sender,
        {mqtt_sender, start_link, [Transport,Socket]},
        permanent,          % cannot recover from a lost connection
        2000,               % should be more than sufficient
        worker,             % as opposed to supervisor
        [mqtt_sender]
    }
).

-define(CONN_SPEC(SenderPid,ReceiverPid,Options),
    {
        connection,                               %% Id
        {mqtt_connection, start_link, [SenderPid,ReceiverPid,Options]},
        permanent,                                %% must never stop
        5000,                                     %% should be more than sufficient for the process to clean up
        worker,                                   %% as opposed to supervisor
        [mqtt_connection]
    }
).

%% API
-export([start_link/0, create_tree/5]).

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
    supervisor:start_link(?MODULE, []).

create_tree(SupPid,ReceiverPid,Transport,Socket,Options) ->
    {ok, SenderPid } = supervisor:start_child(SupPid,
                        ?SENDER_SPEC(Transport,Socket)),
    {ok, ConnPid} = supervisor:start_child(SupPid,
                        ?CONN_SPEC(SenderPid,ReceiverPid,Options)),
    {ok, ConnPid}.

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
    {ok, {{one_for_all, 0, 1}, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

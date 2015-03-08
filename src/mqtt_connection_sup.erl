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

-define(SENDER_SPEC(Transport,Socket),
	{
		sender,
		{mqtt_sender, start_link, [Transport,Socket]},
		temporary,          % cannot recover from a lost connection
		2000,               % should be more than sufficient
		worker,             % as opposed to supervisor
		[mqtt_sender]
	}
).

-define(CONN_SPEC(SenderPid,Options),
	{
		connection,                               %% Id
		{mqtt_connection, start_link, [SenderPid,Options]},
		temporary,                                %% must never stop
		5000,                                     %% should be more than sufficient for the process to clean up
		worker,                                   %% as opposed to supervisor
		[mqtt_connection]
	}
).

-define(RECEIVER_SPEC(TRS,ConnPid,Opts),
	{
		receiver,
		{mqtt_parser_server, start_link, [TRS,ConnPid,Opts]},
		temporary,          % must never stop
		2000,               % should be more than sufficient
		worker,             % as opposed to supervisor
		[mqtt_parser_server]
	}
).



%% API
-export([start_link/2]).

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
start_link(TRS = {Transport,_Ref,Socket},Options) ->
	SupPid = supervisor:start_link(?MODULE, []), %% Will return after both Sender and Receiver have been initialized
	{ok, SenderPid } = supervisor:start_child(SupPid,
		?SENDER_SPEC(Transport,Socket)),
	{ok, ConnPid} = supervisor:start_child(SupPid,
		?CONN_SPEC(SenderPid,Options)),
	{ok, _ReceiverPid } = supervisor:start_child(SupPid,
		?RECEIVER_SPEC(TRS,ConnPid, Options)),
	{ok,SupPid}.

%% start_link(Options,Security,Transport,_Ref,Socket,ReceiverPid) ->
%%   {ok,SupPid} = supervisor:start_link(?MODULE, []), %% Will return after both Sender and Receiver have been initialized
%%   {ok, SenderPid } = supervisor:start_child(SupPid, ?SENDER_SPEC(Transport,Socket)),
%%   {ok, _ConnPid} = supervisor:start_child(SupPid, ?CONN_SPEC(SenderPid,Options)),
%%   {ok,SupPid}.

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

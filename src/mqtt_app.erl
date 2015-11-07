-module(mqtt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([start/0, install/1, install/2, wait_for_tables/1]).



start() ->
    application:ensure_all_started(mqtt).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    init(),

    TransOpts = [
        {port,1883}
    ],
    _SslTransOpts = [
        {port,5556},
        {certfile, ""},
        {cacertfile, ""},
        {verify, verify_peer}
    ],
    ProtOpts = [
        {shutdown,5000},
        %%{connection_type, supervisor} %% mqtt_ranch_sup is an adaptor for the mqtt_connection_sup
        {connection_type, worker} %% mqtt_ranch_sup is an adaptor for the mqtt_connection_sup
    ],
    {ok,_} = ranch:start_listener(mqtt, 1000, ranch_tcp, TransOpts, mqtt_ranch_sup, ProtOpts),
    %%{ok,_} = ranch:start_listener(mqtt_ssl, 1000, ranch_ssl, SslTransOpts, mqtt_ranch_sup, ProtOpts),
    mqtt_sup:start_link().

stop(_State) ->
    ok.

%% ===================================================================
%% Misc
%% ===================================================================

init() ->
    Nodes = [node()],
    ok = mnesia:start(),
    wait_for_tables(Nodes).

wait_for_tables(_Nodes) ->
    [Mod:wait_for_tables() || Mod <- get_tables()].

install(Nodes) ->
    install(Nodes,1).

install(Nodes,Frags) ->

    error_logger:info_msg("Creating mnesia schema"),
    mnesia:create_schema(Nodes),
    rpc:multicall(Nodes, application, ensure_started, [mnesia]),
    error_logger:info_msg("Mnesia started on all nodes"),

    [Mod:create_tables(Nodes,Frags) || Mod <- get_tables()],

    error_logger:info_msg("Installation complete").

get_tables() ->
    [
        mqtt_sub_repo,
        mqtt_reg_repo,
        mqtt_filter_index,
        mqtt_topic_repo,
        mqtt_session_repo
    ].


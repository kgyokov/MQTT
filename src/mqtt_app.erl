-module(mqtt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, install/1, install/2, wait_for_tables/1]).

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
    mqtt_sub_repo:wait_for_tables(),
    mqtt_reg_repo:wait_for_tables(),
    mqtt_filter_index:wait_for_tables(),
    mqtt_topic_repo:wait_for_tables().

install(Nodes) ->
    install(Nodes,1).

install(Nodes,Frags) ->
    error_logger:info_msg("Creating mnesia schema"),
    mnesia:create_schema(Nodes),
    rpc:multicall(Nodes, application, ensure_started, [mnesia]),
    error_logger:info_msg("Mnesia started on all nodes"),

    mqtt_sub_repo:create_tables(Nodes,Frags),
    mqtt_reg_repo:create_tables(Nodes,Frags),
    mqtt_filter_index:create_tables(Nodes,Frags),
    mqtt_topic_repo:create_tables(Nodes,Frags),
    mqtt_session_repo:create_tables(Nodes,Frags),

    error_logger:info_msg("Installation complete").

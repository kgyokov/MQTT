-module(mqtt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, install/1, install/2, wait_for_tables/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    TransOpts = [
        {port,1883}
    ],
    SslTransOpts = [
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


wait_for_tables(Nodes) ->
    mqtt_sub_repo:wait_for_tables().

install(Nodes) ->
    install(Nodes,1).

install(Nodes,Frags) ->
    mnesia:create_schema(Nodes),
    rpc:multicall(Nodes, application, ensure_started, [mnesia]),

    mqtt_sub_repo:create_tables(Nodes,Frags).
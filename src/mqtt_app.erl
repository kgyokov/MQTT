-module(mqtt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  TransOpts = [
    {port,5555}
  ],
  SslTransOpts = [
    {port,5556},
    {certfile, ""},
    {cacertfile, ""},
    {verify, verify_peer}
  ],
  ProtOpts = [
    {shutdown,5000},
    {connection_type, supervisor} %% mqtt_ranch_sup is an adaptor for the mqtt_connection_sup
  ],
  {ok,_} = ranch:start_listener(mqtt, 1000, ranch_tcp, TransOpts, mqtt_ranch_sup, ProtOpts),
  {ok,_} = ranch:start_listener(mqtt_ssl, 1000, ranch_ssl, SslTransOpts, mqtt_ranch_sup, ProtOpts),
  mqtt_sup:start_link().

stop(_State) ->
    ok.

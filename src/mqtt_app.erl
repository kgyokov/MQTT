-module(mqtt_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok,_} = ranch:start_listener(mqtt, 100, ranch_tcp, [{port,5555}],mqtt_connection_sup1,[]),
    %%io:write(Return),
    mqtt_sup:start_link().

stop(_State) ->
    ok.

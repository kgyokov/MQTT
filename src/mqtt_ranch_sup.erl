%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%  Simple Adaptor mapping ranch_protocol to mqtt_connection_sup
%%% @end
%%% Created : 20. Feb 2015 12:40 AM
%%%-------------------------------------------------------------------
-module(mqtt_ranch_sup).
-author("Kalin").

-behaviour(ranch_protocol).

%% API
-export([start_link/4]).

start_link(Ref, Socket, Transport, Opts) ->
    TRS = {Transport,Ref,Socket},
    mqtt_receiver:start_link(TRS,Opts).

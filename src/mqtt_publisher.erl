%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Dec 2014 8:13 PM
%%%-------------------------------------------------------------------
-module(mqtt_publisher).
-author("Kalin").

-compile(export_all).
%% API
-export([]).



publish_at_most_once(Topic,PacketId,Message)->
  0.


publish_at_least_once(Topic,PacketId,Message)->
  0.


%%
%%
%%  Persists the message, getting ready to send it
%%
%%
publish_exactly_once_phase1(Topic,PacketId,Message)->
  0.

%%
%%
%%  Completes message send
%%
%%
publish_exactly_once_phase2(Topic,PacketId,Message)->
  0.

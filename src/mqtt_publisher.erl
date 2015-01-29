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

-include("mqtt_pubsub.hrl").

-compile(export_all).
%% API
-export([]).



at_most_once(Msg,S)->
  #mqtt_message{topic = Topic} = Msg,
  #mqtt_session{incoming_qos0 = List} = S,
  %% replace by Topic
  S#mqtt_session{incoming_qos0 = [Msg|List]}
 .



at_least_once(Topic,PacketId,Content,Retain)->
%%   #mqtt_session{incoming_qos1 = List} = S,
%%   S#mqtt_session{incoming_qos2 = [Msg|List]}
0.


%%
%%
%%  Persists the message, getting ready to send it
%%
%%
exactly_once_phase1(Topic,PacketId,Content,Retain)->
  0.

%%
%%
%%  Completes message send
%%
%%
exactly_once_phase2(PacketId)->
  0.

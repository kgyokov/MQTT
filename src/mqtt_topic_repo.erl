%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Dec 2014 2:13 AM
%%%-------------------------------------------------------------------
-module(mqtt_topic_repo).
-author("Kalin").

%% API
-export([]).

-record(mqtt_sub, {topic, fan_in, clients =[]}).






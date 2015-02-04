%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jan 2015 1:02 AM
%%%-------------------------------------------------------------------
-module(mqtt_topic).
-author("Kalin").

%% API
-export([explode_split_topic/1]).

%%
%% Expands a topic to any possible wildcard match



-record(high_fan_in, {topic, subscribers = []}).


%%
%% @doc
%% Explodes a topic into the various possible patterns that can match it
%% e.g.   /user/1234/location :
%% /#
%% /user/#
%% /user/1234/#
%% /user/1234/location
%% /user/1234/+
%% /user/+/location
%% /user/+/+
%% /+/1234/location
%% /+/1234/+
%% /+/+/location
%% /+/+/+
%%
%% This ensures quick matching to high fan-in subscriptions, e.g. /user/#
%% @end
%%
explode_topic(TopicLevels)->
  explode_topic([],TopicLevels).

explode_topic(ParentLevels,["/"|T])->
  {
    explode_topic(["/"|ParentLevels],T),
    ["#","/"|ParentLevels]
  }
;

explode_topic(ParentLevels,[Level|T])->
  {
    explode_topic([Level|ParentLevels],T),
    explode_topic(["+"|ParentLevels],T)
  }
;

explode_topic(ParentLevels,[])->
  ParentLevels
.

split_topic(Topic) ->
  split_topic([],Topic)
.

split_topic(Split,<<>>)->
  Split
;

split_topic(Split,<<"/"/utf8,Rest>>)->
  split_topic(["/"|Split],Rest)
;

split_topic(Split,<<Rest>>)->
  {NextLevel,Rest1} = consume_level(Rest),
  split_topic([NextLevel|Split],Rest1)
.

consume_level(Binary)->
  consume_level([],Binary).

consume_level([],<<"/"/utf8,_>>) ->
  throw({error,empty_level});

consume_level(Level,Rest = <<"/"/utf8,_>>) ->
  {Level,Rest};

consume_level(Level,Rest = <<>>) ->
  {Level,Rest};

consume_level(Level,Rest = <<NextChar:1/utf8,Rest>>) ->
  {[NextChar|Level],Rest}.


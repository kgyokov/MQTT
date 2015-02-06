%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Topic Utilities
%%% @end
%%% Created : 20. Jan 2015 1:02 AM
%%%-------------------------------------------------------------------
-module(mqtt_topic).
-author("Kalin").

%% API
-export([explode_topic/1, is_covered_by/2]).

%%
%% Expands a topic to any possible wildcard match



-record(high_fan_in, {topic, subscribers = []}).


%% @doc
%% Tells us if second topic pattern covers the first one.
%% Examples
%% /user/+/location covers /user/123/location
%% /user/# covers /user/123/location
%% /user/123/+ does NOT cover /user/+/location
%% /user/123/location does NOT cover /user/123/+
%%
%% @end
is_covered_by(Pattern,Cover)->
  PL = lists:reverse(split_topic(Pattern)),
  CL = lists:reverse(split_topic(Cover)),
  seg_is_covered_by(PL,CL)
.


seg_is_covered_by([PH|PT],[CH|CT])->
  case {PH,CH} of
    {_,"#"}->
      true;
    {"#",_}->
      false;
    {_,"+"}->
      seg_is_covered_by(PT,CT);
    {PH,PH}->
      seg_is_covered_by(PT,CT);
    _ ->
      false
  end;

seg_is_covered_by([],["#"])->
  true;
seg_is_covered_by([],[_|_])->
  false;
seg_is_covered_by([],[])->
  true;
seg_is_covered_by([_|_],[])->
  false.

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
  [lists:reverse(RL) || RL <- explode_topic([],TopicLevels)].

explode_topic(ParentLevels,["/"|T])->
  [
    ["#","/"|ParentLevels] |
    explode_topic(["/"|ParentLevels],T)
  ]
;

explode_topic(ParentLevels,[Level|T])->
    explode_topic([Level|ParentLevels],T) ++ explode_topic(["+"|ParentLevels],T)
;

explode_topic(ParentLevels,[])->
  ParentLevels
.

%% @doc
%% Splits a topic pattern based on delimiter
%% /user/123/location -> ["/",<<"user">>,"/",<<"123>>,"/",<<"location">>]
%%
%% @end
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

consume_level(Level,Rest = <<NextChar/utf8,Rest>>) ->
  {[NextChar|Level],Rest}.


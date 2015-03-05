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
-export([explode_topic/1, is_covered_by/2, split_topic/1, distinct/1]).


%% Topic patterns form a Partially-ordered set through the 'is_covered_by' relationship.
%% Runs O(N^2)
distinct(Covers) ->
	distinct([],Covers).

distinct(Maximals,[]) ->
	Maximals;

distinct(Maximals,[H|T]) ->
	distinct(merge_max(Maximals,H),T).

merge_max(Maximals,NewMax) ->
	%% DedupL = lists:filter(fun(Max) -> not is_covered_by(Max,NewMax)  end, Maximals),
	DedupL = [ Max || Max <- Maximals, not is_covered_by(Max,NewMax) ],
	case lists:any(fun(Max) -> is_covered_by(NewMax,Max) end, DedupL) of
		true    -> DedupL;
		false   -> [NewMax|DedupL]
	end.

%% @doc
%% Tells us if the 2nd topic pattern covers the 1st one.
%% Examples
%% /user/+/location covers /user/123/location
%% /user/# covers /user/123/location
%% /user/123/+ does NOT cover /user/+/location
%% /user/123/location does NOT cover /user/123/+
%%
%% @end
is_covered_by(Pattern,Cover)->
  PL = split_topic(Pattern),
  CL = split_topic(Cover),
  seg_is_covered_by(PL,CL).

seg_is_covered_by(_,["/","#"])  -> true;  %%  '/#' definitely covers '_' (everything)
seg_is_covered_by(_,["#"])      -> true;  %%  '#' definitely covers '_' (everything)
seg_is_covered_by([],[_|_])     -> false; %% else if the pattern is longer than the potential match, there is no match
seg_is_covered_by([_|_],[])     -> false; %% else if the patter is shorter than the potential match
seg_is_covered_by([],[])        -> true;  %% if the pattern is as long as the potential match, THIS IS a match

%% # > + > char
seg_is_covered_by([PH|PT],[CH|CT])->
	case {PH,CH} of
		{_,"#"} -> true;
		{"#",_} -> false;
		{_,"+"} -> seg_is_covered_by(PT,CT);
		{PH,PH} -> seg_is_covered_by(PT,CT);
		_       -> false
	end.


%% @doc
%% Number of possible matching subscriptions is O(N^2), where N is the number of levels
%%
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

explode_topic(<<TopicLevels/binary>>)->
  explode_topic(split_topic(TopicLevels));

explode_topic(TopicLevels) when is_list(TopicLevels)->
%%   lists:map(
%%     fun(Topic)->
%%       list_to_binary(lists:map(
%%         fun(Level)->
%%
%%         end,
%%         Topic))
%%     end).
  %lists:reverse(explode_topic([],TopicLevels)).
  %explode_topic([],TopicLevels).
  [ list_to_binary(lists:reverse(RL)) || RL <- explode_topic([],TopicLevels)].

explode_topic(ParentLevels,["/"|T]) ->
  [
    ["#","/"|ParentLevels] |
    explode_topic(["/"|ParentLevels],T)
  ];

explode_topic(ParentLevels,[Level|T]) ->
    explode_topic([Level|ParentLevels],T) ++
    explode_topic(["+"|ParentLevels],T);

explode_topic(ParentLevels,[])->
  [ParentLevels].

%% @doc
%% Splits a topic pattern based on delimiter
%% /user/123/location -> ["/",<<"user">>,"/",<<"123>>,"/",<<"location">>]
%%
%% @end
split_topic(Topic) ->
  lists:reverse(split_topic([],Topic))
.

split_topic(Split,<<>>) ->
  Split;

split_topic(["#"|_],_Rest) ->
  throw({error,invalid_wildcard});

split_topic(Split,<<"/"/utf8,Rest/binary>>) ->
  split_topic(["/"|Split],Rest);

split_topic(Split,<<Rest/binary>>) ->
  {NextLevel,Rest1} = consume_level(Rest),
  split_topic([NextLevel|Split],Rest1).



consume_level(Binary) ->
  consume_level(<<>>,Binary).

consume_level(<<>>,<<"#"/utf8>>) ->                   {"#",<<>>};
consume_level(_,<<"#"/utf8,_/binary>>) ->             throw({error,unexpected_wildcard});
consume_level(<<>>,<<"+"/utf8,Rest/binary>>) ->       {"+",Rest};
consume_level(Level,Rest = <<"/"/utf8,_/binary>>) ->  {Level,Rest};
consume_level(Level,<<>>) ->                          {Level,<<>>};
consume_level(Level,<<NextCh/utf8,Rest/binary>>) ->   consume_level(<<Level/binary,NextCh/utf8>>,Rest).





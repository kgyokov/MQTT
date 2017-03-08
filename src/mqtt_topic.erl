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
-export([explode/1, is_covered_by/2, split/1, min_cover/1, best_match/2]).


%% @doc
%%Determines the minimum subset of Filters that covers the same topics
%% e.g. [ /A/B/+ , /A/B/C ] can be reduced to [/A/B/+]
%% @end
min_cover(Filters) ->
    lists:foldl(fun maybe_add_new_max/2,[],Filters).

maybe_add_new_max(NewMax,Maximals) ->
    DedupL = [ Max || Max <- Maximals, not is_covered_by(Max,NewMax) ],
    case lists:any(fun(Max) -> is_covered_by(NewMax,Max) end, DedupL) of
        true    -> DedupL;
        false   -> [NewMax|DedupL]
    end.


%% @doc
%% Picks a the best matching Sub with highest QoS.
%% Best means one that:
%%      - matches the Topic
%%      - has the highest QoS
%%      - Plus some arbitrary criteria
%% @end
best_match(Subs,Topic) ->
    Matches = lists:filter(fun({Filter,_}) -> is_covered_by(Topic,Filter) end, Subs),
    case Matches of
        [_|_] -> {ok, match_with_max_qos(Matches)};
        []    -> error
    end.

match_with_max_qos(Subs = [H|_]) ->
    lists:foldl(fun(El,Acc) ->
                    case is_better_match(El,Acc) of
                        true  -> El;
                        false -> Acc
                    end
                end,H,Subs).

is_better_match({Filter,QoS},{MinFilter,MaxQoS}) ->
    QoS > MaxQoS
        orelse (QoS == MaxQoS andalso
        (is_covered_by(Filter,MinFilter)
            orelse (not is_covered_by(MinFilter,Filter)
                andalso Filter < MinFilter)
        )
    ).

%% normalize(<<Pattern/binary>>) ->
%%     LPattern = split(Pattern),
%%     list_to_binary(normalize(LPattern));
%%
%% normalize(Ptn) when is_list(Ptn) ->
%%     RPtn = lists:reverse(Ptn),
%%     NPtn = lists:reverse(normalize_r(RPtn)),
%%     case NPtn of
%%         ["/","#"|T2] -> ["#",T2];
%%         _           -> NPtn
%%     end.

normalize_r(RPtn) ->
    case RPtn of
        ["#"|T1] -> ["#" | eliminate_w(T1)];
        _       -> RPtn
    end.

eliminate_w(["/","+"|T]) ->
    eliminate_w(T);

eliminate_w(T) ->
    T.

is_covered_by({Pattern1,QoS1},{Pattern2,QoS2}) ->
    is_covered_by(Pattern1,Pattern2) andalso QoS2 >= QoS1;

%% @doc
%% Tells us if the Cover covers the Pattern.
%% Examples
%% /user/+/location covers /user/123/location
%% /user/# covers /user/123/location
%% /user/123/+ does NOT cover /user/+/location
%% /user/123/location does NOT cover /user/123/+
%%
%% @end
is_covered_by(Pattern,Cover)->
    PL = split(Pattern),
    CL = split(Cover),
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

-spec explode(binary()|list(binary())) -> [binary()].
explode(<<TopicLevels/binary>>)->
    explode(split(TopicLevels));

explode(TopicLevels) when is_list(TopicLevels) ->
    RawList = [ list_to_binary(lists:reverse(RL)) || RL <- explode([],TopicLevels)],
    Set = sets:from_list(RawList),
    [<<"#">>|sets:to_list(Set)].

explode(ParentLevels,["/"|T]) ->
    [
        ["#","/"|ParentLevels] |
        explode(["/"|ParentLevels],T)
    ];

explode(ParentLevels,[Level|T]) ->
    explode([Level|ParentLevels],T) ++
    explode(["+"|ParentLevels],T);

explode(ParentLevels,[])->
    [ParentLevels].

%% @doc
%% Splits a topic pattern based on delimiter
%% /user/123/location -> ["/",<<"user">>,"/",<<"123>>,"/",<<"location">>]
%%
%% @end
split(Pattern) ->
    lists:reverse(split([],Pattern)).

split(Acc,<<>>) ->
    Acc;

split(["#"|_],_Rest) ->
    throw({error,invalid_wildcard});

split(Acc,<<"/">>) ->
    ["/"|Acc];

split(Acc,<<"/",Rest/binary>>) ->
    Acc1 = ["/"|Acc],
    {NextLevel,Rest1} = consume_level(Rest),
    split([NextLevel|Acc1],Rest1);

split(Acc,<<Rest/binary>>) ->
    {NextLevel,Rest1} = consume_level(Rest),
    split([NextLevel|Acc],Rest1).


consume_level(Binary) ->
    consume_level(<<>>,Binary).

consume_level(<<>>,  <<"#">>)                       ->  {"#",<<>>};
consume_level(_,     <<"#",_/binary>>)              ->  throw({error,unexpected_wildcard});
consume_level(<<>>,  <<"+",Rest/binary>>)           ->  {"+",Rest};
consume_level(Level, Rest = <<"/",_/binary>>)       ->  {Level,Rest};
consume_level(Level, <<>>)                          ->  {Level,<<>>};
consume_level(Level, <<NextCh/utf8,Rest/binary>>)   ->  consume_level(<<Level/binary,NextCh/utf8>>,Rest).





%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Feb 2015 10:36 PM
%%%-------------------------------------------------------------------
-module(mqtt_topic_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").

-define(TEST_MODULE, mqtt_topic).
-define(assertListsEqual(List1,List2),
 [] = (List1)--(List2),
 [] = (List2)--(List1)
).

-define(assertListsEqual2(List1,List2),
  (?_test((?assertEqual(((List1)--(List2)) ++ ((List2)--(List1)),[]))))
).

-define(_assertListsEqual(List1,List2),?_test(?assertListsEqual(List1,List2))).

-compile([exportall]).

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


split_topic_test_()->
  [
    ?_assertEqual(["/",<<"user"/utf8>>,"/",<<"1234"/utf8>>,"/",<<"location"/utf8>>],
      mqtt_topic:split_topic(<<"/user/1234/location"/utf8>>))
   ].

explode_topic_test_() ->
[
  ?_test(lists_are_equal([
    <<"/#"/utf8>>,
    <<"/user/#"/utf8>>,
    <<"/user/1234/#"/utf8>>,
    <<"/user/1234/location"/utf8>>,
    <<"/user/1234/+"/utf8>>,
    <<"/user/+/location"/utf8>>,
    <<"/user/+/+"/utf8>>,
    <<"/+/1234/location"/utf8>>,
    <<"/+/1234/+"/utf8>>,
    <<"/+/+/location"/utf8>>,
    <<"/+/+/+"/utf8>>
  ],
    mqtt_topic:explode_topic(<<"/user/1234/location"/utf8>>))),
  ?_test(lists_are_equal([
    <<"#"/utf8>>,
    <<"user/#"/utf8>>,
    <<"user/1234/#"/utf8>>,
    <<"user/1234/location"/utf8>>,
    <<"user/1234/+"/utf8>>,
    <<"user/+/location"/utf8>>,
    <<"user/+/+"/utf8>>,
    <<"+/1234/location"/utf8>>,
    <<"+/1234/+"/utf8>>,
    <<"+/+/location"/utf8>>,
    <<"+/+/+"/utf8>>
  ],
    mqtt_topic:explode_topic(<<"user/1234/location/"/utf8>>))),
  ?_test(lists_are_equal([
    <<"#"/utf8>>,
    <<"user/#"/utf8>>,
    <<"user/1234/#"/utf8>>,
    <<"user/1234/location/"/utf8>>,
    <<"user/1234/+/"/utf8>>,
    <<"user/+/location/"/utf8>>,
    <<"user/+/+/"/utf8>>,
    <<"+/1234/location/"/utf8>>,
    <<"+/1234/+/"/utf8>>,
    <<"+/+/location/"/utf8>>,
    <<"+/+/+/"/utf8>>
  ],
    mqtt_topic:explode_topic(<<"user/1234/location"/utf8>>)))
].

is_covered_by_test_()->
  [
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/user/123/location">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/user/123/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/user/123/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/user/+/location">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/user/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/user/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/+/123/location">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/+/123/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/+/123/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/+/+/location">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"/+/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/user/123/location">>,<<"#">>))
  ]
.

lists_are_equal(List1,List2)->
  ?assertEqual([],(List1 -- List2) ++ (List2 -- List1)).


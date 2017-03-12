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

%% normalize_test_() ->
%% 	[
%% 		?_assertEqual(mqtt_topic:normalize(<<"/#"/utf8>>),<<"#"/utf8>>),
%% 		?_assertEqual(mqtt_topic:normalize(<<"/A/+/#"/utf8>>),<<"/A/#"/utf8>>),
%% 		?_assertEqual(mqtt_topic:normalize(<<"/+/+/#"/utf8>>),<<"#"/utf8>>)
%% 	].

distinct_test_() ->
	[
		?_test(lists_are_equal(
								[],
			mqtt_topic:min_cover([])
		)),
		?_test(lists_are_equal(
			mqtt_topic:min_cover([<<"/A/1/B">>]),
			[<<"/A/1/B"/utf8>>]
		)),
		?_test(lists_are_equal(
			mqtt_topic:min_cover([<<"/A/1/B">>,<<"/A/2/B">>]),
			[<<"/A/1/B"/utf8>>,<<"/A/2/B">>]
		)),
		?_test(lists_are_equal(
			mqtt_topic:min_cover([<<"/A/1/B">>,<<"/A/2/B">>,<<"/A/1/+">>]),
			[<<"/A/2/B"/utf8>>,<<"/A/1/+">>]
		)),
		?_test(lists_are_equal(
			mqtt_topic:min_cover([<<"/A/1/B">>,<<"/A/2/B">>,<<"/A/+/B">>]),
			[<<"/A/+/B"/utf8>>]
		)),
		?_test(lists_are_equal(
			mqtt_topic:min_cover([<<"/A/1/B">>,<<"/A/2/B">>,<<"/A/+/B">>,<<"/A/3/C">>]),
			[<<"/A/+/B"/utf8>>,<<"/A/3/C">>]
		)),
		?_test(lists_are_equal(
			mqtt_topic:min_cover([<<"/A/1/B">>,<<"/A/2/B">>,<<"/A/+/B">>,	<<"/A/3/C">>,
								<<"/A/4/C">>,<<"/A/+/C">>,<<"/A/#">>]),
			[<<"/A/#"/utf8>>]
		))
	].

split_test_()->
  [
    ?_assertEqual(
      ['/',<<"A">>,'/',<<"1">>,'/',<<"B">>],
      mqtt_topic:split(<<"/A/1/B"/utf8>>)),
    ?_assertEqual(
      [<<"A">>,'/',<<"1">>,'/',<<"B">>,'/'],
      mqtt_topic:split(<<"A/1/B/">>)),
    ?_assertEqual(
      [<<"A">>,'/',<<"1">>,'/','#'],
      mqtt_topic:split(<<"A/1/#">>)),
    ?_assertEqual(
      [<<"A">>,'/',<<>>,'/'],
      mqtt_topic:split(<<"A//">>))
   ].

explode_test_() ->
[
  ?_test(lists_are_equal([
    <<"#">>,
    <<"/#">>,
    <<"/A/#">>,
    <<"/A/1/#">>,
    <<"/A/1/B">>,
    <<"/A/1/+">>,
    <<"/A/+/#">>,
    <<"/A/+/B">>,
    <<"/A/+/+">>,
    <<"/+/#">>,
	<<"/+/1/#">>,
    <<"/+/1/B">>,
    <<"/+/1/+">>,
    <<"/+/+/#">>,
    <<"/+/+/B">>,
    <<"/+/+/+">>
  ],
    mqtt_topic:explode(<<"/A/1/B">>))),
  ?_test(lists_are_equal([
    <<"#">>,
    <<"A/#">>,
    <<"A/1/#">>,
    <<"A/1/B/">>,
	<<"A/1/B/#">>,
    <<"A/1/+/">>,
    <<"A/1/+/#">>,
    <<"A/+/#">>,
    <<"A/+/B/">>,
	<<"A/+/B/#">>,
    <<"A/+/+/#">>,
    <<"A/+/+/">>,
    <<"+/#">>,
    <<"+/1/B/">>,
	<<"+/1/B/#">>,
	<<"+/1/#">>,
    <<"+/1/+/#">>,
    <<"+/1/+/">>,
    <<"+/+/#">>,
    <<"+/+/B/">>,
	<<"+/+/B/#">>,
    <<"+/+/+/#">>,
    <<"+/+/+/">>
  ],
    mqtt_topic:explode(<<"A/1/B/">>))),
  ?_test(lists_are_equal([
    <<"#">>,
    <<"A/#">>,
    <<"A/1/#">>,
    <<"A/1/B">>,
    <<"A/1/+">>,
    <<"A/+/#">>,
    <<"A/+/B">>,
    <<"A/+/+">>,
    <<"+/#">>,
	<<"+/1/#">>,
    <<"+/1/B">>,
    <<"+/1/+">>,
    <<"+/+/#">>,
    <<"+/+/B">>,
    <<"+/+/+">>
  ],
    mqtt_topic:explode(<<"A/1/B">>)))
].

is_covered_by_test_()->
  [
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/B">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B/">>,<<"/A/1/B/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/B/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/+/B">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/1/B">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/1/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/+/B">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"#">>)),

    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/B/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/X">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/2/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/X/1/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/A/1/B/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/X/+/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/+/X">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/2/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/+/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/B">>,<<"/+/+">>)),


    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/1/B/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/1/B/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/1/+/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/+/B/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"A/+/+/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"+/1/B/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"+/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"+/1/+/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"+/+/B/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"+/+/+/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"#">>)),


    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/A/1/B/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/A/1/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/A/1/X/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/A/2/B/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/X/1/B/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/A/1/B/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/X/+/+/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/+/+/X/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/+/2/+/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/+/+/+/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"A/1/B/">>,<<"/+/+/+">>)),


    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/1/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/+/1/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/+/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"#">>)),

    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/1/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/+/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/+/2/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/X/1/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/2/#">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/+/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/+">>,<<"/A/+">>)),


    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/+/B">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/+/+/B">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/+/+/+">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"#">>)),

    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/1/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/X/+/B">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/+/X">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/+/+/X">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/X/+/+">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/+/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/+/B">>,<<"/A/+">>)),

    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/A/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/A/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/+/1/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/+/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/+/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"#">>)),

    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/A/X/#">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/X/1/#">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/1/#">>,<<"/A/1/+">>)),

    %% empty topics
    ?_assert(mqtt_topic:is_covered_by(<<"/A//">>,<<"/A//">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A//">>,<<"/A/+/">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A//">>,<<"/A/#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A//">>,<<"/A//#">>)),
    ?_assert(mqtt_topic:is_covered_by(<<"/A///">>,<<"/A/+/+/">>)),

    ?_assertNot(mqtt_topic:is_covered_by(<<"/A//">>,<<"/A///">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A//">>,<<"/A/">>)),
    ?_assertNot(mqtt_topic:is_covered_by(<<"/A/">>,<<"/A//#">>))
  ].

best_match_test_() ->
    Subs = [{<<"A/B">>,1},
            {<<"A/B">>,2},
            {<<"A/+">>,1},
            {<<"+/+">>,2}],
    [
    ?_assertEqual({ok,{<<"+/+">>,2}},mqtt_topic:best_match(Subs,<<"A/C">>)),
    ?_assertEqual({ok,{<<"A/B">>,2}},mqtt_topic:best_match(Subs,<<"A/B">>)),
    ?_assertEqual(error,mqtt_topic:best_match(Subs,<<"A/B/C">>))
].

lists_are_equal(List1,List2)->
  ?assertEqual([],(List1 -- List2) ++ (List2 -- List1)).


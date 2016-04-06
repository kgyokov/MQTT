%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Apr 2016 10:30 PM
%%%-------------------------------------------------------------------
-module(shared_set_test).
-author("Kalin").

-include("test_utils.hrl").

setup_shared_set() ->
    shared_set:new().

all_test_() ->
        [
            fun empty_after_creation/0,
            fun error_when_accessing_at_0_index/0,
            fun empty_result_when_accessing_at_less_than_smallest_index/0,
            fun correct_result_when_accessing_at_smallest_index/0,
            fun correct_result_when_accessing_at_largest_index/0,
            fun correct_result_when_accessing_above_largest_index/0,
            fun truncate_below_minimum_leaves_set_intact/0,
            fun truncate_at_maximum_leaves_last_element/0,
            fun truncate_above_maximum_leaves_last_element/0,
            fun correct_result_when_truncating/0,
            fun remove_erases_the_key_from_corresponding_version/0,
            fun removing_same_key_twice/0
        ].


empty_after_creation() ->
    N = shared_set:new(),
    ?assertEqual(0,length((shared_set:get_at(1,N)))),
    ?assertEqual(0,length(shared_set:get_at(2,N))).

error_when_accessing_at_0_index() ->
    N = shared_set:new(),
    ?assertError(_,length(shared_set:get_at(0,N))).

empty_result_when_accessing_at_less_than_smallest_index() ->
    N = shared_set:new(),
    N1 = shared_set:append(key1,val1,2,N),
    ?assertEqual(0,length(shared_set:get_at(1,N1))).

correct_result_when_accessing_at_smallest_index() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    ValAt2 = shared_set:get_at(2,N2),
    ?assertEqual(1,length(ValAt2)),
    ?lists_are_equal([val2],ValAt2).

correct_result_when_accessing_at_largest_index() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    ValAt3 = shared_set:get_at(3,N2),
    ?assertEqual(2,length(ValAt3)),
    ?lists_are_equal([val2,val3],ValAt3).

correct_result_when_accessing_above_largest_index() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    ValAt3 = shared_set:get_at(4,N2),
    ?assertEqual(2,length(ValAt3)),
    ?lists_are_equal([val2,val3],ValAt3).

truncate_below_minimum_leaves_set_intact() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    Truncated = shared_set:truncate(1,N2),
    ?assertEqual(2,shared_set:size(Truncated)).

truncate_above_maximum_leaves_last_element() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    Truncated = shared_set:truncate(4,N2),
    ?assertEqual(0,shared_set:size(Truncated)).

truncate_at_maximum_leaves_last_element() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    Truncated = shared_set:truncate(3,N2),
    ?assertEqual(1,shared_set:size(Truncated)),
    ?lists_are_equal([val2,val3],shared_set:get_at(3,Truncated)).

correct_result_when_truncating() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    N3 = shared_set:append(key4,val4,4,N2),
    Truncated = shared_set:truncate(3,N3),
    ?assertEqual(2,shared_set:size(Truncated)),
    ?lists_are_equal([val2,val3,val4],shared_set:get_at(4,Truncated)).

remove_erases_the_key_from_corresponding_version() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    N3 = shared_set:append(key4,val4,4,N2),
    N4 = shared_set:remove(key3,5,N3),
    ?lists_are_equal([val2,val4],shared_set:get_at(5,N4)),
    ?lists_are_equal([val2,val3,val4],shared_set:get_at(4,N3)).

removing_same_key_twice() ->
    N = shared_set:new(),
    N1 = shared_set:append(key2,val2,2,N),
    N2 = shared_set:append(key3,val3,3,N1),
    N3 = shared_set:append(key4,val4,4,N2),
    N4 = shared_set:remove(key3,5,N3),
    N5 = shared_set:remove(key3,6,N4),
    ?lists_are_equal([val2,val4],shared_set:get_at(6,N5)).

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Apr 2016 10:30 PM
%%%-------------------------------------------------------------------
-module(versioned_set_test).
-author("Kalin").

-include("test_utils.hrl").

all_test_() ->
        [
            fun default_result_when_accessing_at_0_index/0,
            fun default_result_when_accessing_at_less_than_smallest_index/0,
            fun correct_result_when_accessing_at_smallest_index/0,
            fun correct_result_when_accessing_at_largest_index/0,
            fun correct_result_when_accessing_above_largest_index/0,
            fun update_below_minimum_leaves_set_intact/0,
            fun truncate_at_maximum_leaves_last_element/0,
            fun truncate_above_maximum_leaves_last_element/0,
            fun correct_result_when_truncating/0
        ].


default_result_when_accessing_at_0_index() ->
    N = versioned_set:new(0,val0),
    ?assertEqual(val0,versioned_set:get_at(0,N)).

default_result_when_accessing_at_less_than_smallest_index() ->
    N = versioned_set:new(2,val0),
    N1 = versioned_set:update(3,fun(_) -> val1 end, N),
    ?assertEqual(val0,versioned_set:get_at(1,N1)).

correct_result_when_accessing_at_smallest_index() ->
    N = versioned_set:new(2,val0),
    N1 = versioned_set:update(3,fun(_) -> val3 end,N),
    ?assertEqual(val0,versioned_set:get_at(2,N1)).

correct_result_when_accessing_at_largest_index() ->
    N = versioned_set:new(0,val0),
    N1 = versioned_set:update(2,fun(_) -> val2 end,N),
    N2 = versioned_set:update(3,fun(_) -> val3 end,N1),
    ?assertEqual(val3,versioned_set:get_at(3,N2)).

correct_result_when_accessing_above_largest_index() ->
    N = versioned_set:new(0,val0),
    N1 = versioned_set:update(2,fun(_) -> val2 end,N),
    N2 = versioned_set:update(3,fun(_) -> val3 end,N1),
    ?assertEqual(val3,versioned_set:get_at(4,N2)).

update_below_minimum_leaves_set_intact() ->
    N = versioned_set:new(2,val0),
    N1 = versioned_set:update(3,fun(_) -> val2 end,N),
    Truncated = versioned_set:truncate(1,N1),
    ?assertEqual(2, versioned_set:size(Truncated)).

truncate_above_maximum_leaves_last_element() ->
    N = versioned_set:new(0,val0),
    N1 = versioned_set:update(2,fun(_) -> val2 end,N),
    N2 = versioned_set:update(3,fun(_) -> val3 end,N1),
    Truncated = versioned_set:truncate(4,N2),
    ?assertEqual(1, versioned_set:size(Truncated)),
    ?assertEqual(val3, versioned_set:get_at(4,Truncated)).

truncate_at_maximum_leaves_last_element() ->
    N = versioned_set:new(0,val0),
    N1 = versioned_set:update(2,fun(_) -> val2 end,N),
    N2 = versioned_set:update(3,fun(_) -> val3 end,N1),
    Truncated = versioned_set:truncate(3,N2),
    ?assertEqual(1, versioned_set:size(Truncated)),
    ?assertEqual(val3, versioned_set:get_at(4,Truncated)).

correct_result_when_truncating() ->
    N = versioned_set:new(0,val0),
    N1 = versioned_set:update(2,fun(_) -> val2 end,N),
    N2 = versioned_set:update(3,fun(_) -> val3 end,N1),
    N3 = versioned_set:update(4,fun(_) -> val4 end,N2),
    Truncated = versioned_set:truncate(3,N3),
    ?assertEqual(2, versioned_set:size(Truncated)),
    ?assertEqual(val4, versioned_set:get_at(4,Truncated)).





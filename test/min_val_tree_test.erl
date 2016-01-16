%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jan 2016 11:45 PM
%%%-------------------------------------------------------------------
-module(min_val_tree_test).
-author("Kalin").

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").


all_test_() ->
        [
            fun min_is_correct_after_several_inserts/0,
            fun min_is_correct_after_single_insert/0,
            fun min_is_none_for_new_tree/0,
            fun min_is_none_after_removal_of_all_elements/0,
            fun removal_on_empty_does_not_error/0,
            fun min_is_updated_after_removing_all_keys_for_old_min/0,
            fun min_stays_same_if_not_all_keys_removed/0,
            fun min_stays_same_if_not_all_keys_removed/0,
            fun min_stays_same_if_inrelated_key_removed/0
        ].

min_is_correct_after_several_inserts() ->
    store_pairs_and_test_min(
        [
            {a,5},
            {b,3},
            {c,7},
            {y,1},
            {z,2},
            {x,1},
            {m,3}
        ],{ok,1}).

min_is_correct_after_single_insert() ->
    store_pairs_and_test_min([{a,5}],{ok,5}).

min_is_none_for_new_tree() ->
    store_pairs_and_test_min([],none).

min_is_none_after_removal_of_all_elements() ->
    T = store_pairs(
        [
        {a,5},
        {b,3},
        {c,7}
    ]),
    ToRemove = [c,a,b,c],
    T1 = remove_keys(ToRemove,T),
    ?assertEqual(min_val_tree:get_min(T1),none).

removal_on_empty_does_not_error() ->
    T = min_val_tree:new(),
    ToRemove = [a,b,c],
    T1 = remove_keys(ToRemove,T),
    ?assertEqual(none, min_val_tree:get_min(T1)).

min_is_updated_after_removing_all_keys_for_old_min() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {n,1},
            {c,7},
            {y,1}
        ],
    T = store_pairs(Pairs),
    ToRemove = [n,y],
    T1 = remove_keys(ToRemove,T),
    ?assertEqual(min_val_tree:get_min(T1),{ok,3}).

min_stays_same_if_not_all_keys_removed() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {n,1},
            {c,7},
            {y,1}
        ],
    T = store_pairs(Pairs),
    ToRemove = [n],
    T1 = remove_keys(ToRemove,T),
    ?assertEqual(min_val_tree:get_min(T1),{ok,1}).

min_stays_same_if_inrelated_key_removed() ->
    min_val_tree:new(),
    Pairs =
        [
            {a,5},
            {b,3},
            {c,7},
            {y,1}
        ],
    T1 = store_pairs(Pairs),
    ToRemove = [b],
    T2 =remove_keys(ToRemove,T1),
    ?assertEqual(min_val_tree:get_min(T2),{ok,1}).

%% ===========================================================================
%% HELPERS
%% ===========================================================================

store_pairs(Pairs) ->
    lists:foldr(fun({Key,Val},T) -> min_val_tree:store(Key,Val,T) end,
        min_val_tree:new(),
        Pairs).

remove_keys(ToRemove,T) ->
    lists:foldr(fun(Key,T) -> min_val_tree:remove(Key,T) end,
        T,ToRemove).

store_pairs_and_test_min(Pairs,ExpectedMin) ->
    T1 = store_pairs(Pairs),
    Min = min_val_tree:get_min(T1),
    ?assertEqual(Min,ExpectedMin).



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
            fun min_is_correct_after_duplicate_key_instered/0,
            fun min_is_none_for_new_tree/0,
            fun min_is_none_after_removal_of_all_elements/0,
            fun removal_on_empty_does_not_error/0,
            fun min_is_updated_after_removing_all_keys_of_old_min/0,
            fun min_stays_same_if_not_all_corresponding_keys_removed/0,
            fun min_stays_same_if_not_all_corresponding_keys_removed/0,
            fun min_stays_same_if_unrelated_key_removed/0,
            %% Guarantees monotonicity of the min value
            fun cannot_update_w_lower_value/0,
            fun cannot_insert_w_lower_value/0,
            fun split_empty_tree/0,
            fun split_tree_w_smaller_than_min_value/0,
            fun split_tree_w_zero/0,
            fun split_tree_general_case/0
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

min_is_correct_after_duplicate_key_instered() ->
    store_pairs_and_test_min(
        [
            {a,3},
            {b,4},
            {a,5}
        ],{ok,4}).

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
    ?assertEqual(min_val_tree:min(T1),none).

removal_on_empty_does_not_error() ->
    T = min_val_tree:new(),
    ToRemove = [a,b,c],
    T1 = remove_keys(ToRemove,T),
    ?assertEqual(none, min_val_tree:min(T1)).

min_is_updated_after_removing_all_keys_of_old_min() ->
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
    ?assertEqual(min_val_tree:min(T1),{ok,3}).

min_stays_same_if_not_all_corresponding_keys_removed() ->
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
    ?assertEqual(min_val_tree:min(T1),{ok,1}).

min_stays_same_if_unrelated_key_removed() ->
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
    ?assertEqual(min_val_tree:min(T2),{ok,1}).

%% Guarantee monotonicity
cannot_update_w_lower_value() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {c,7}
        ],
    T1 = store_pairs(Pairs),
    T2 = min_val_tree:store(a,1,T1),
    ?assertEqual(min_val_tree:min(T2),{ok,3}),
    ?assertEqual(min_val_tree:get_val(a,T2),{ok,5}).

%% Guarantee monotonicity
cannot_insert_w_lower_value() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {c,7}
        ],
    T1 = store_pairs(Pairs),
    T2 = min_val_tree:store(b,1,T1),
    ?assertEqual({ok,3},min_val_tree:min(T2)),
    ?assertEqual({ok,3},min_val_tree:get_val(b,T2)).

split_empty_tree() ->
    T1 = min_val_tree:new(),
    {L,T2} = min_val_tree:split(2,T1),
    ?assertEqual(length(L),0),
    ?assert(min_val_tree:is_empty(T2)).

split_tree_w_smaller_than_min_value() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {c,7}
        ],
    T1 = store_pairs(Pairs),
    {L,T2} = min_val_tree:split(5,T1),
    ?assertEqual(3,length(L)),
    ?assert( min_val_tree:is_empty(T2)).

split_tree_w_zero() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {c,7}
        ],
    T1 = store_pairs(Pairs),
    {L,T2} = min_val_tree:split(0,T1),
    ?assertEqual(0,length(L)),
    ?assert(not min_val_tree:is_empty(T2)).

split_tree_general_case() ->
    Pairs =
        [
            {a,5},
            {b,3},
            {d,2},
            {c,7}
        ],
    T1 = store_pairs(Pairs),
    {L,T2} = min_val_tree:split(2,T1),
    ?assertEqual([{d,2},{b,3}],L),
    ?assertEqual({ok,5},min_val_tree:min(T2)).

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
    Min = min_val_tree:min(T1),
    ?assertEqual(Min,ExpectedMin).

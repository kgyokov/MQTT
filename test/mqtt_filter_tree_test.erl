%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Mar 2017 5:22 AM
%%%-------------------------------------------------------------------
-module(mqtt_filter_tree_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").
-include("test_utils.hrl").

simple_test() ->
    Tree = mqtt_filter_tree:new(),
    Tree1 = mqtt_filter_tree:update_raw_filter(['/',<<"A">>,'/','+'],Tree,fun(_) -> [a] end),
    Tree2 = mqtt_filter_tree:update_raw_filter(['/',<<"A">>,'/',<<"B">>],Tree1,fun(_) -> [b] end),
    Tree3 = mqtt_filter_tree:update_raw_filter(['/',<<"C">>,'/',<<"B">>],Tree2,fun(_) -> [c] end),
    Tree4 = mqtt_filter_tree:update_raw_filter(['/','#'],Tree3,fun(_) -> [d] end),
    Tree5 = mqtt_filter_tree:update_raw_filter(['/',<<"A">>,'/','+'],Tree4,fun(L) -> [e|L] end),
    error_logger:info_msg(Tree5),
    Val = mqtt_filter_tree:get_vals_for_filter(['/',<<"A">>,'/','+'],Tree5),
    Iter = mqtt_filter_tree:get_iterator_for_matching_filters(['/',<<"A">>,'/',<<"B">>],Tree5),
    {Solution1,Iter1} = mqtt_filter_tree:next_filter_match(Iter),
    {Solution2,Iter2} = mqtt_filter_tree:next_filter_match(Iter1),
    {Solution3,Iter3} = mqtt_filter_tree:next_filter_match(Iter2),
    Nil = mqtt_filter_tree:next_filter_match(Iter3),
    ?assertEqual([e,a],Val),
    ?lists_are_equal([[e,a],[b],[d]],[Solution1,Solution2,Solution3]),
    ?assertEqual(nil,Nil).


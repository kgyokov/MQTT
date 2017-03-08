%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Apr 2016 10:48 PM
%%%-------------------------------------------------------------------
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").

-define(lists_are_equal(List1,List2),
    ?assertEqual([],(List1 -- List2) ++ (List2 -- List1))).
%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Feb 2015 10:09 PM
%%%-------------------------------------------------------------------
-module(iter_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").

simple_test() ->
    Seq =
        [   iter:take(10,iter:seq(1)),
            iter:take(5,iter:seq(11)),
            iter:take(5,iter:seq(16)),
            iter:from_list([])
        ],
    Seq1 = iter:from_list(Seq),
    Seq2 = iter:concat(Seq1),
    Seq3 = iter:map(fun(El) -> El + 20 end, Seq2),
    ?assertEqual(lists:seq(21,40),iter:to_list(Seq3)).

flatten_test() ->
    List = [
        iter:take(5,iter:seq(1)),
        iter:take(5,iter:seq(6)),
        iter:take(5,iter:seq(11)),
        iter:from_list([])
    ],
    ListIter= iter:from_list(List),
    FLatIter = iter:flatten(ListIter),
    ?assertEqual(lists:seq(1,15), iter:to_list(FLatIter)).

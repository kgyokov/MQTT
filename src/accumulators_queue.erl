%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Mar 2017 10:56 PM
%%%-------------------------------------------------------------------
-module(accumulators_queue).
-author("Kalin").

-behavior(gen_accumulator).

%% API
-export([acc/2, id/0, get_tree/1, get_seq/1]).

-define(ACCUMULATORS,[accumulator_gb_tree,accumulator_seq]).

acc(El,Acc) -> lists:zipwith(fun(Acc1,Mod) -> Mod:acc(El,Acc1) end,Acc,?ACCUMULATORS).

id() -> [ Mod:id() || Mod <- ?ACCUMULATORS].

get_tree([Tree,_]) -> Tree.

get_seq([_,Seq]) -> Seq.
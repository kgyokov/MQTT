%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Mar 2017 11:05 PM
%%%-------------------------------------------------------------------
-module(accumulator_seq).
-author("Kalin").

-behavior(gen_accumulator).

%% API
-export([acc/2, id/0]).


acc(El, _) -> El.

id() -> 0.
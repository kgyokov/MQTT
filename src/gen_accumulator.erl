%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Mar 2017 11:17 PM
%%%-------------------------------------------------------------------
-module(gen_accumulator).
-author("Kalin").

-callback acc(_,Acc) -> Acc.
-callback id() -> any().
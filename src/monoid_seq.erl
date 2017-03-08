%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Apr 2016 7:38 PM
%%%-------------------------------------------------------------------
-module(monoid_seq).
-author("Kalin").

-behavior(gen_monoid).

%% API
-export([id/0, as/2, ms/1]).


id() -> 0.
as(A, B) -> max(A,B).
ms({Seq,_,_}) -> Seq.

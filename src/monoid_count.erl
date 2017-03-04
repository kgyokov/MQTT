%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Mar 2017 8:29 PM
%%%-------------------------------------------------------------------
-module(monoid_count).
-author("Kalin").

-behavior(gen_monoid).

%% API
-export([id/0, as/2, ms/1]).


id() -> 0.
as(A, B) -> A + B.
ms(_) -> 1.
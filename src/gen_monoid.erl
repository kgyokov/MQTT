%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 11:51 PM
%%%-------------------------------------------------------------------
-module(gen_monoid).
-author("Kalin").

-callback id()    -> any().
-callback as(A,A) -> A.
-callback ms(_)   -> _.
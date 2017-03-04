%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Mar 2016 12:23 AM
%%%-------------------------------------------------------------------
-module(mqtt_monoid_queue).
-author("Kalin").

-behavior(monoid).

%% API
-export([id/0, as/2, ms/1]).

-define(MONOIDS,[monoid_sequence,monoid_not_qos0]).

id()    -> [M:id()     || M <- ?MONOIDS].
as(A,B) -> [M:as(A,B)  || M <- ?MONOIDS].
ms(A)   -> [M:ms(A)    || M <- ?MONOIDS].
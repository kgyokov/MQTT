%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Mar 2016 12:23 AM
%%%-------------------------------------------------------------------
-module(mqtt_queue_monoid).
-author("Kalin").

-behavior(gen_monoid).

%% API
-export([id/0, as/2, ms/1]).

-define(MONOIDS,[sequence_monoid,not_qos0_monoid]).

id() -> [ M:id() || M <- ?MONOIDS].
as(A,B) -> [ M:as(A,B) || M <- ?MONOIDS].
ms(A) -> [ M:ms(A) || M <- ?MONOIDS].
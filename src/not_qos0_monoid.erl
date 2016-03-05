%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Mar 2016 12:21 AM
%%%-------------------------------------------------------------------
-module(not_qos0_monoid).
-author("Kalin").

-behavior(gen_monoid).

%% API
-export([id/0, as/2, ms/1]).

id() -> false.
as(NotQoS0_1,NotQoS0_2) -> NotQoS0_1 orelse NotQoS0_2.
ms(QoS) -> QoS =/= 0.


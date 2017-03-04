%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 11:45 PM
%%%-------------------------------------------------------------------
-module(monoid_sequence).
-author("Kalin").

-behavior(gen_monoid).

-export([id/0, as/2, ms/1]).
-export([split_by_seq/2,pushr_w_seq/3,get_monoid_val/2,extract_val/1]).

-include("mqtt_internal_msgs.hrl").
-include("finger_tree.hrl").

%% MONOID implementation

%% @doc
%% @todo: optimize
%% @end
-define(MONOIDS,[monoid_seq,monoid_not_qos0]).

id()    -> [M:id()     || M <- ?MONOIDS].
as(A,B) -> [M:as(A,B)  || M <- ?MONOIDS].
ms(A)   -> [M:ms(A)    || M <- ?MONOIDS].

get_monoid_val(seq,     [Val,_]) -> Val;
get_monoid_val(not_qos0,[_,Val]) -> Val.

%% Helper methods

pushr_w_seq(Seq,El,Q) ->
    pushr(Q,{Seq,El}).

split_by_seq(Fun,Q) ->
    split(fun({Seq,_}) -> Fun(Seq) end,Q).

extract_val({_,Val}) -> Val.

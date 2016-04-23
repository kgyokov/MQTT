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
-export([id/0, as/2, ms/1,
    split_by_seq/2, pushr_w_seq/3, annotate_w_seq/2, get_seq/1, get_no_qos0/1, get_count/1]).

-include("finger_tree.hrl").

%% MONOID implementation

id() -> {0,0,0}.
as({MaxSeq1,Count1,NotQoS0_1},
   {MaxSeq2,Count2,NotQoS0_2}) ->
    {max(MaxSeq1,MaxSeq2),
     Count1 + Count2,
     NotQoS0_1 + NotQoS0_2}.
ms({Seq,{QoS,_Rest}}) ->
    {Seq,
        1,
        case QoS of
            0 -> 0;
            _ -> 1
        end}.


%% Helper methods

get_seq({Seq,_,_}) -> Seq.
get_count({_,Count,_}) -> Count.
annotate_w_seq(Seq,El) -> {Seq,El}.
get_no_qos0({_,_,NotQoS0}) -> NotQoS0.

pushr_w_seq(Seq,El,Q) ->
    pushr(Q,{Seq,El}).

split_by_seq(Fun,Q) ->
    split(fun({Seq,_,_}) -> Fun(Seq) end,Q).

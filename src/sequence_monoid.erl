%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 11:45 PM
%%%-------------------------------------------------------------------
-module(sequence_monoid).
-author("Kalin").

-behavior(gen_monoid).
-export([id/0, as/2, ms/1,
    split_by_seq/2, pushr_w_seq/3]).

-include("finger_tree.hrl").

%% MONOID implementation

id() -> {0,false}.
as({Seq1,NotQoS0_1},{Seq2,NotQoS0_2}) ->
    {max(Seq1,Seq2),
        NotQoS0_1 orelse NotQoS0_2}.
ms({Seq,{QoS,_El}}) ->
    {Seq,QoS =/= 0}.


%% Helper methods

pushr_w_seq(Seq,El,Q) ->
    pushr({Seq,El},Q).

split_by_seq(Fun,Q) ->
    split(fun({Seq,_}) -> Fun(Seq) end,Q).
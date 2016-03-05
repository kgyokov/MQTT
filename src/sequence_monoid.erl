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
-export([id/0,as/2,ms/1]).

-include("finger_tree.hrl").

%% API

id() -> 0.
as({Seq1,NotQoS0_1},{Seq2,NotQoS0_2}) ->
    { max(Seq1,Seq2),NotQoS0_1 andalso NotQoS0_2}.
ms({Seq,{QoS,_El}}) ->
    {Seq,QoS =/= 0}.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Compares composite message sequence numbers.
%%% Each
%%% @end
%%% Created : 24. Mar 2016 8:45 PM
%%%-------------------------------------------------------------------
-module(mqtt_seq).
-author("Kalin").

-type bottom() :: bottom.

%% @doc
%% The monotonically incrementing sequence of a client's subscription
%% We maintain two sequence integers per subscription:
%% RetSeq - a sequence number for all sent retained messages (i.e. messages sent as a result of a 'SUB' request
%% QSeq - a sequence number for all messages sent during a normal flow
%% @end
-type subscription_seq() :: {RetSeq::non_neg_integer(),
                             QSeq::non_neg_integer()}| bottom().

%% API
-export([bottom/0, new/2, inc_retained/1, inc_queued/1, compare/2, max/2, min/2, get_ret_seq/1, get_q_seq/1]).

get_ret_seq({RetSeq,_}) -> RetSeq.
get_q_seq({_,QSeq}) -> QSeq.

-spec inc_retained(subscription_seq()) -> subscription_seq().

inc_retained({RetSeq,QSeq}) -> {RetSeq+1,QSeq}.
inc_queued({RetSeq,QSeq})   -> {RetSeq,QSeq+1}.

-spec bottom()-> undefined.
bottom() -> undefined.

-spec new(non_neg_integer(),non_neg_integer())-> subscription_seq().

new(RetSeq,QSeq) -> {RetSeq,QSeq}.

-spec compare(subscription_seq(),subscription_seq()) -> lt|gt|eq.
compare(Seq1,Seq1)           -> eq;
compare(_,undefined)         -> gt;
compare(undefined,_)         -> lt;

compare({_,QSeq1},{_,QSeq2})             when QSeq1 > QSeq2                        -> gt;
compare({RetSeq1,QSeq1},{RetSeq2,QSeq2}) when QSeq1 =:= QSeq2, RetSeq1 > RetSeq2   -> gt;
compare(_,_)                                                                       -> lt.

-spec max(subscription_seq(),subscription_seq()) -> subscription_seq().
max(Seq1,Seq2) ->
    case compare(Seq1,Seq2) of
        gt -> Seq1;
        _ -> Seq2
    end.

-spec min(subscription_seq(),subscription_seq()) -> subscription_seq().
min(Seq1,Seq2) ->
    case compare(Seq1,Seq2) of
        gt -> Seq2;
        _ -> Seq1
    end.
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

%% @doc
%% The monotonically incrementing sequence of a client's subscription
%% We maintain two sequence integers per subscription:
%% RetSeq - a sequence number for all sent retained messages (i.e. messages sent as a result of of a 'SUB' request
%% QSeq - a sequence number for all messages sent during a normal flow
%% @end
-type subscription_seq() :: {RetSeq::non_neg_integer()|-1,
                             QSeq::non_neg_integer()|-1}.

%% @doc
%% The Sequence number of a message
%% The first element of the tuple indicates the type of the message - retained or normal queue message
%% The second element indicates the corresponding integer
%% @end
-type msg_seq() :: {ret|q,Seq::non_neg_integer()}.

%% API
-export([is_new_msg/2, bottom/0, new/2, compare/2, inc_retained/1, inc_queued/1, make_seq/2]).

%% @doc
%% Compares the sequence number of a message to that of a subscription.
%% @-spec is_new_msg :: (msg_seq(), subscription_seq()) -> bool
%% @end

-spec is_new_msg(msg_seq(), subscription_seq()) -> boolean().

is_new_msg({ret,Seq},{RetSeq,_}) -> Seq > RetSeq;
is_new_msg({q,Seq},{_,QSeq})     -> Seq > QSeq.

select(ret) -> fun({Seq,_}) -> Seq end;
select(q)   -> fun({_,Seq}) -> Seq end.

-spec inc_retained(subscription_seq()) -> subscription_seq().

inc_retained({RetSeq,QSeq}) -> {RetSeq+1,QSeq}.
inc_queued({RetSeq,QSeq})   -> {RetSeq,QSeq+1}.

-spec bottom()-> {-1,-1}.

bottom() -> new(-1,-1).

-spec new(non_neg_integer()|-1,non_neg_integer()|-1)->subscription_seq().

new(RetSeq,QSeq) -> {RetSeq,QSeq}.

-spec compare(subscription_seq(),subscription_seq()) -> gt|eq|lt|unknown.

compare({RetSeq1,QSeq1},{RetSeq2,QSeq2}) ->
    Diff1 = compare_num(RetSeq1,RetSeq2),
    Diff2 = compare_num(QSeq1,QSeq2),
    case Diff1 =:= Diff2 of
        true  -> Diff1;
        false -> unknown
    end.

compare_num(Seq1,Seq2) when Seq1 > Seq2     -> gt;
compare_num(Seq1,Seq2) when Seq1 =:= Seq2   -> eq;
compare_num(Seq1,Seq2) when Seq1 < Seq2     -> lt.

-spec make_seq(IsRetained::boolean(),Seq::non_neg_integer()) -> msg_seq().

make_seq(_IsRetained = true,Seq)  -> {ret,Seq};
make_seq(_IsRetained = false,Seq) -> {q,Seq}.




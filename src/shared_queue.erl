%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Represents a queue shared by different consumers
%%% @end
%%% Created : 02. Mar 2016 11:38 PM
%%%-------------------------------------------------------------------
-module(shared_queue).
-author("Kalin").

%% API
-export([new/0, new/1,new/2, pushr/2, get_current_seq/1, take/3, split_by_seq/2, get_queue/1, get_front_acc/1, get_back_acc/1, take_values/3, truncate/2, get_min_offset/1]).

-define(DEFAULT_SEQ,0).
-define(ACCUMULATORS,accumulator_gb_tree).
-define(MONOID,monoid_sequence).

%%-record(shared_q,{
%%    last_seq :: non_neg_integer(),       %% Sequence number of the latest item added to the queue
%%                                        %% incremented with each new item
%%                                        %% (represented as the corresponding item's Sequence number)
%%    queue   :: {any(),any(),any()}                %% the actual queue
%%}).

new() -> new(?DEFAULT_SEQ).

%% @doc
%% Creates a new shared queue starting from Seq, using the
%% Comp comparison function
%% @end
new(Seq) ->
    new(Seq,?ACCUMULATORS:id()).

new(Seq,StartAcc) ->
    {Seq,StartAcc,StartAcc,monoid_sequence:empty()}.

pushr(El,{Seq,AccF,AccB,Q}) ->
    Seq1 = Seq + 1,
    AccB1 = ?ACCUMULATORS:acc(El,AccB),
    {Seq1,AccF,AccB1,monoid_sequence:pushr(Q,{Seq1,El,AccB1})}.

split_by_seq(Fun,{Seq,AccF,AccB,Q}) ->
    {First,Second} = monoid_sequence:split_by_seq(Fun,Q),
    {Seq2,AccB2} =
        case monoid_sequence:is_empty(First) of
            true -> {Seq,AccF};
            false ->
                {LastSeq,_,LastAcc} = monoid_sequence:headr(First),
                {LastSeq,LastAcc}
        end,

    {{Seq,AccF,AccB2,First},{Seq2,AccB2,AccB,Second}}.

take(AfterSeq,Num,SQ) ->
    {_,Rest} = split_by_seq(fun(Seq) -> Seq  > AfterSeq end,SQ),
    {Interval,_} = split_by_seq(fun(Seq) -> Seq > AfterSeq + Num end,Rest),
    Interval.

truncate(AfterSeq,SQ) ->
    {{_,_,_,QF},Rest} = split_by_seq(fun(Seq) -> Seq  > AfterSeq end,SQ),
    {monoid_sequence:ms(QF),Rest}.

%%forward(ClientId,ToSeq,SQ = #shared_q{offsets = Offsets}) ->
%%    Offsets1 = min_val_tree:store(ClientId,ToSeq,Offsets),
%%    maybe_truncate(Offsets1,SQ).
%%
%%remove(ClientId,SQ = #shared_q{offsets = Offsets}) ->
%%    Offsets1 = min_val_tree:remove(ClientId,Offsets),
%%    maybe_truncate(Offsets1,SQ).
%%
%%add_client(ClientId,SQ = #shared_q{last_seq = CurSeq}) ->
%%    add_client(ClientId,CurSeq,SQ).
%%
%%add_client(ClientId,AtSeq,SQ = #shared_q{offsets = Offsets}) ->
%%    MinSeq = min_offset(SQ),
%%    ActualSeq = max(MinSeq,AtSeq),
%%    SQ#shared_q{offsets = min_val_tree:store(ClientId,ActualSeq,Offsets)}.
%%
%%maybe_truncate(NewOffsets, SQ = #shared_q{queue = Q}) ->
%%    MinSeq = min_offset(SQ),
%%    {{_,_,Garbage},Q1} = split_by_seq(fun(Seq) -> Seq >= MinSeq end,Q),
%%    {monoid_sequence:measure(Garbage),
%%        SQ#shared_q{queue = Q1, offsets = NewOffsets}}.
%%
%%%% @doc
%%%% Returns the minimum sequence number being referenced by the clients
%%%% @end
%%min_offset(#shared_q{offsets = Offsets, last_seq = LastSeq}) ->
%%    case min_val_tree:min(Offsets) of
%%        none -> LastSeq + 1;
%%        {ok,Seq}  -> Seq
%%    end.


get_queue({_,_,_,Q})        -> Q.
get_front_acc({_,AccF,_,_}) -> AccF.
get_back_acc({_,_,AccB,_})  -> AccB.

%% @todo: Maybe use accumulators for the min and max Sequence numbers?!

get_current_seq({Seq,_,_,_}) -> Seq.
get_min_offset({Seq,_,_,Q}) ->
    case monoid_sequence:is_empty(Q) of
        true -> Seq;
        false ->
            {SeqFirst,_,_} = monoid_sequence:headl(Q),
            SeqFirst
    end
.

take_values(AfterSeq,Num,{_,_,_,Q}) ->
    {_,Rest}     = monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq end, Q),
    {Interval,_} = monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq + Num end, Rest),
    lists:map(fun({_,Val,_}) -> Val end, monoid_sequence:to_list(Interval)).




%%add_seq(infinity,_) -> infinity;
%%add_seq(_,infinity) -> infinity;
%%add_seq(_,infinity) -> infinity;
%%add_seq(Seq,Num) -> Seq + Num.
%%
%%gte(infinity) -> fun(_) -> false end;
%%gte(AfterSeq) -> fun(Seq) -> Seq >= AfterSeq end.
%%
%%gt(infinity) -> fun(_) -> false end;
%%gt(AfterSeq) -> fun(Seq) -> Seq > AfterSeq end.

%%split_after_seq(infinity,Q) -> Q;
%%split_after_seq(AfterSeq,Q) -> monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq end, Q).


%%
%% Private functions
%%

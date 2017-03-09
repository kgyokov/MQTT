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
-export([new/2,new/3,new/4, pushr/2, get_current_seq/1, take/3, split_by_seq/2, get_queue/1,
    get_front_acc/1, get_back_acc/1, take_values/3, truncate/2, get_min_offset/1]).

-record(sq,{
    accmod :: module(),
    monoidmod ::module(),
    seq :: non_neg_integer(),
    accf :: any(),
    accb :: any(),
    q :: any()
}).

new(MonMod,AccMod) -> new(0,MonMod,AccMod).

%% @doc
%% Creates a new shared queue starting from Seq, using the
%% Comp comparison function
%% @end
new(Seq,MonMod,AccMod) ->
    new(Seq,AccMod:id(),MonMod,AccMod).

new(Seq,StartAcc,MonMod,AccMod) ->
    #sq{monoidmod = MonMod,accmod = AccMod,
        seq = Seq,accf = StartAcc,accb = StartAcc,q = MonMod:empty()}.

pushr(El,SQ = #sq{accmod = AccMod,monoidmod = MonMod,
                  seq = Seq,accb = AccB,q = Q}) ->
    Seq1 = Seq + 1,
    AccB1 = AccMod:acc(El,AccB),
    SQ#sq{seq = Seq1,accb = AccB1,q = MonMod:pushr(Q,{Seq1,El,AccB1})}.

split_by_seq(Fun,SQ = #sq{monoidmod = MonMod,
                          seq = Seq,accf = AccF,q = Q}) ->
    {First,Second} = MonMod:split_by_seq(Fun,Q),
    AccB1 =
        case MonMod:is_empty(First) of
            true -> AccF;
            false ->
                {_,_,LastAcc} = MonMod:headr(First),
                LastAcc
        end,
    Seq1 = get_inner_min_offset(MonMod,Seq,Q),
    {SQ#sq{seq = Seq1,accb = AccB1,q = First},
     SQ#sq{accf = AccB1,q = Second}}.

take(AfterSeq,Num,SQ) ->
    {_,Rest}     = split_by_seq(fun(Seq) -> Seq > AfterSeq end,SQ),
    {Interval,_} = split_by_seq(fun(Seq) -> Seq > AfterSeq + Num end,Rest),
    Interval.

truncate(AfterSeq,SQ = #sq{monoidmod = MonMod}) ->
    {#sq{q = QF},Rest} = split_by_seq(fun(Seq) -> Seq  > AfterSeq end,SQ),
    {MonMod:measure(QF),Rest}.

get_queue(#sq{q = Q})        -> Q.
get_front_acc(#sq{accf = AccF}) -> AccF.
get_back_acc(#sq{accb = AccB})  -> AccB.

%% @todo: Maybe use accumulators for the min and max Sequence numbers?!

get_current_seq(#sq{seq = Seq}) -> Seq.
get_min_offset(#sq{seq = Seq,q = Q,monoidmod = MonMod}) ->
    get_inner_min_offset(MonMod,Seq,Q).

get_inner_min_offset(MonMod,Seq,Q) ->
    case MonMod:is_empty(Q) of
        true -> Seq;
        false ->
            {SeqFirst,_,_} = MonMod:headl(Q),
            SeqFirst-1
    end.

take_values(AfterSeq,Num,#sq{monoidmod = MonMod, q = Q}) ->
    {_,Rest}     = MonMod:split_by_seq(fun(Seq) -> Seq > AfterSeq end, Q),
    {Interval,_} = MonMod:split_by_seq(fun(Seq) -> Seq > AfterSeq + Num end, Rest),
    lists:map(fun({_,Val,_}) -> Val end, MonMod:to_list(Interval)).




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

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
-export([new/0, new/1, pushr/2, remove/2, add_client/2, add_client/3, forward/3, min_offset/1, max_offset/1, take/3, split_by_seq/2, get_queue/1, get_front_acc/1, get_back_acc/1]).

-define(DEFAULT_SEQ,0).
-define(ACCUMULATORS,accumulator_gb_tree).

-record(shared_q,{
    last_seq :: non_neg_integer(),       %% Sequence number of the latest item added to the queue
                                        %% incremented with each new item
    offsets :: min_val_tree:tree(binary(),non_neg_integer()), %% How far each consumer is pointing into the queue
                                        %% (represented as the corresponding item's Sequence number)
    queue   :: {any(),any(),any()}                %% the actual queue
}).


%% Fragmented queues - one for disk, one for memory
take_clients_before(Seq,Offsets) ->
    {min_val_tree:iterator(Offsets),Seq}.

next({Iter,Seq}) ->
    case min_val_tree:next(Iter) of
        {ClientId,Val} when Val =< Seq -> {ClientId,Val};
        _ -> nil
    end.

new() -> new(?DEFAULT_SEQ).

%% @doc
%% Creates a new shared queue starting from Seq, using the
%% Comp comparison function
%% @end
new(Seq) ->
    new(Seq,?ACCUMULATORS:id()).

new(Seq,StartAcc) ->
    #shared_q{offsets  = min_val_tree:new(),
              queue    = {StartAcc,StartAcc,monoid_sequence:empty()},
              last_seq = Seq}.

pushr(El,SQ = #shared_q{last_seq = Seq, queue = Q, offsets = Offsets}) ->
    Seq1 = Seq+1,
    Q1 = case min_val_tree:is_empty(Offsets) of
             true -> pushr_w_acc(Seq1,El,Q);
             false -> Q
         end,
    SQ#shared_q{last_seq = Seq1,queue = Q1}.

pushr_w_acc(Seq,El,{AccF,AccB,Q}) ->
    AccB1 = ?ACCUMULATORS:acc(El,AccB),
    {AccF,AccB1,monoid_sequence:pushr({Seq,El,AccB1},Q)}.

split_by_seq(Fun,{AccF,AccB,Q}) ->
    {First,Second} = monoid_sequence:split(fun({Seq,_,_}) -> Fun(Seq) end,Q),
    AccB2 =
        case monoid_sequence:is_empty(First) of
            true -> AccF;
            false -> monoid_sequence:headr(Second)
            end,
    {{AccF,AccB2,First},{AccB2,AccB,Second}}.

forward(ClientId,ToSeq,SQ = #shared_q{offsets = Offsets}) ->
    Offsets1 = min_val_tree:store(ClientId,ToSeq,Offsets),
    maybe_truncate(Offsets1,SQ).

remove(ClientId,SQ = #shared_q{offsets = Offsets}) ->
    Offsets1 = min_val_tree:remove(ClientId,Offsets),
    maybe_truncate(Offsets1,SQ).

add_client(ClientId,SQ = #shared_q{last_seq = CurSeq}) ->
    add_client(ClientId,CurSeq,SQ).

add_client(ClientId,AtSeq,SQ = #shared_q{offsets = Offsets}) ->
    MinSeq = min_offset(SQ),
    ActualSeq = max(MinSeq,AtSeq),
    SQ#shared_q{offsets = min_val_tree:store(ClientId,ActualSeq,Offsets)}.

maybe_truncate(NewOffsets, SQ = #shared_q{queue = Q}) ->
    MinSeq = min_offset(SQ),
    {{_,_,Garbage},Q1} = split_by_seq(fun(Seq) -> Seq >= MinSeq end,Q),
    {monoid_sequence:measure(Garbage),
        SQ#shared_q{queue = Q1, offsets = NewOffsets}}.

get_queue(#shared_q{queue = {_,_,Q}})        -> Q.
get_front_acc(#shared_q{queue = {AccF,_,_}}) -> AccF.
get_back_acc(#shared_q{queue = {_,AccB,_}})  -> AccB.

%% @doc
%% Returns the minimum sequence number being referenced by the clients
%% @end
min_offset(#shared_q{offsets = Offsets, last_seq = LastSeq}) ->
    case min_val_tree:min(Offsets) of
        none -> LastSeq + 1;
        {ok,Seq}  -> Seq
    end.

max_offset(#shared_q{last_seq = Seq}) -> Seq.

take(AfterSeq,Num,#shared_q{queue = Q}) ->
    {_,Rest}     = monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq end, Q),
    {Interval,_} = monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq + Num end, Rest),
    lists:map(fun monoid_sequence:extract_val/1, monoid_sequence:to_list(Interval)).

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

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
-export([new/1, pushr/2, move/3, min_seq/1, max_seq/1, read/3, remove/2, add/2, add/3]).

-record(shared_q,{
    cur_seq :: non_neg_integer(), %% Sequence number of the latest item added to the queue
    %% incremented with each new item
    client_seqs :: any(),            %% how far each consumer is into the queue
    %% (represented as the corresponding item's Sequence number)
    queue       :: any()             %% the actual queue
}).

new(Seq) ->
    #shared_q{client_seqs = min_val_tree:new(),
              queue = sequence_monoid:empty(),
              cur_seq = Seq}.

pushr(El,SQ = #shared_q{cur_seq = Seq,queue = Q}) ->
    Seq1 = Seq+1,
    SQ#shared_q{cur_seq = Seq1,queue = sequence_monoid:pushr_w_seq(Seq1,El,Q)}.

move(ClientId,ToSeq,SQ = #shared_q{client_seqs = Offsets}) ->
    Offsets1 = min_val_tree:store(ClientId,ToSeq,Offsets),
    maybe_truncate(Offsets1,SQ).

remove(ClientId,SQ = #shared_q{client_seqs = Offsets}) ->
    Offsets1 = min_val_tree:remove(ClientId,Offsets),
    maybe_truncate(Offsets1,SQ).

add(ClientId,SQ = #shared_q{cur_seq = CurSeq}) ->
    add(ClientId,CurSeq,SQ).

add(ClientId,Seq,SQ = #shared_q{client_seqs = Offsets,cur_seq = CurSeq}) ->
    %%@todo: Should we sanitize the input???
    ActualSeq = max(min_val_tree:min(Offsets),min(CurSeq,Seq)),
    SQ#shared_q{client_seqs = min_val_tree:store(ClientId,ActualSeq,Offsets)}.


maybe_truncate(NewOffsets, SQ = #shared_q{queue = Q}) ->
    MinClientSeq = min_val_tree:min(NewOffsets),
    {_,Q1} = sequence_monoid:split(fun(Seq) -> Seq >= MinClientSeq end,Q),
    SQ#shared_q{queue = Q1,client_seqs = NewOffsets}.

min_seq(#shared_q{client_seqs = Offsets}) -> min_val_tree:min(Offsets).

max_seq(#shared_q{cur_seq = Seq}) -> Seq.

read(MinSeq,MaxSeq, #shared_q{queue = Q}) ->
    {_,Rest} =      sequence_monoid:split_by_seq(fun(Seq) -> Seq >= MinSeq end, Q),
    {Interval,_} =  sequence_monoid:split_by_seq(fun(Seq) -> Seq =< MaxSeq end, Rest),
    sequence_monoid:to_list(Interval).


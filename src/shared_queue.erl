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
-export([new/0, new/1, pushr/2, remove/2, add_client/2, add_client/3, forward/3, min_seq/1, max_seq/1, take/3]).

-define(SEQ_MONOID,sequence_monoid).
-define(DEFAULT_SEQ,0).

-record(shared_q,{
    last_seq :: non_neg_integer(),       %% Sequence number of the latest item added to the queue
                                        %% incremented with each new item
    client_seqs :: min_val_tree:tree(binary(),non_neg_integer()), %% How far each consumer is pointing into the queue
                                        %% (represented as the corresponding item's Sequence number)
    queue       :: any()                %% the actual queue
}).

new() -> new(?DEFAULT_SEQ).

%% @doc
%% Creates a new shared queue starting from Seq, using the
%% Comp comparison function
%% @end
new(Seq) ->
    #shared_q{client_seqs = min_val_tree:new(),
              queue       = monoid_sequence:empty(),
              last_seq    = Seq}.

pushr(El,SQ = #shared_q{last_seq = Seq,queue = Q}) ->
    Seq1 = Seq+1,
    SQ#shared_q{last_seq = Seq1,queue = monoid_sequence:pushr_w_seq(Seq1,El,Q)}.

forward(ClientId,ToSeq,SQ = #shared_q{client_seqs = Offsets}) ->
    Offsets1 = min_val_tree:store(ClientId,ToSeq,Offsets),
    maybe_truncate(Offsets1,SQ).

%%whereis(ClientId,#shared_q{client_seqs = Offsets}) ->
%%    min_val_tree:get_val(ClientId,Offsets).

remove(ClientId,SQ = #shared_q{client_seqs = Offsets}) ->
    Offsets1 = min_val_tree:remove(ClientId,Offsets),
    maybe_truncate(Offsets1,SQ).

add_client(ClientId,SQ = #shared_q{last_seq = CurSeq}) ->
    add_client(ClientId,CurSeq,SQ).

add_client(ClientId,AtSeq,SQ = #shared_q{client_seqs = Offsets, last_seq = CurSeq}) ->
    %%@todo: Should we sanitize the input???
    MinVal = case min_val_tree:min(Offsets) of
                none -> ?DEFAULT_SEQ;
                {ok,Seq} -> Seq
            end,
    ActualSeq = max(MinVal,min(CurSeq,AtSeq)),
    SQ#shared_q{client_seqs = min_val_tree:store(ClientId,ActualSeq,Offsets)}.

maybe_truncate(NewOffsets, SQ = #shared_q{queue = Q}) ->
    MinClientSeq = min_val_tree:min(NewOffsets),
    {Garbage,Q1} = monoid_sequence:split_by_seq(fun(Seq) -> Seq >= MinClientSeq end,Q),
    {monoid_sequence:measure(Garbage),
        SQ#shared_q{queue = Q1,client_seqs = NewOffsets}}.

min_seq(Q = #shared_q{client_seqs = Offsets}) ->
    case min_val_tree:min(Offsets) of
        none -> max_seq(Q);
        {ok,Seq}  -> Seq
    end.

max_seq(#shared_q{last_seq = Seq}) -> Seq.

take(AfterSeq, Num, #shared_q{queue = Q}) ->
    {_,Rest}     = monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq end, Q),
    {Interval,_} = monoid_sequence:split_by_seq(fun(Seq) -> Seq > AfterSeq + Num end, Rest),
    lists:map(fun monoid_sequence:extract_val/1, monoid_sequence:to_list(Interval)).

%%
%% Private functions
%%

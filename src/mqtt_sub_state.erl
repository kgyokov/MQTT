%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Handles the state of a client's subscription to a particular filter
%%% @end
%%% Created : 06. Mar 2016 1:09 AM
%%%-------------------------------------------------------------------
-module(mqtt_sub_state).
-author("Kalin").

-include("mqtt_internal_msgs.hrl").

%% API
-export([take/3,
    take_any/2,
    new/3,
    new/4,
    reset/3,
    is_old/2]).

-record(sub, {
    qos                                 ::qos(), %% QoS for this client subscription
    client_seq = 0                      ::non_neg_integer(), %% the version number of the client process registration
    %% (incremented every time a new client process is spawned, used to choose
    %% between different instances of a Client in case of race conditions)
    %% last_ack = 0  ::non_neg_integer(),   %% the filter-assigned sequence number of the last message processed by this client,
    idx = {0,0}                         :: {non_neg_integer(),non_neg_integer()},
    window = 0                          ::non_neg_integer(),    %% How many messages the client has requested
    retained_iter                       :: versioned_set:iter() %% the retained messages to send to the client
}).

is_old(MsgCSeq,#sub{client_seq = CSeq}) when CSeq >= MsgCSeq -> true;
is_old(_,_) -> false.

%% @doc
%%
%% @end
take_any(Q,Sub) ->
    take(0,Q,Sub).

%% @doc
%% Determine the packets to send depending on the size of the Client's Window.
%% Called under one the following conditions:
%% - The Client has consumed one or more package and has increased its window size
%% - There may be new messages to send
%% @end
take(ToTake,Q,Sub = #sub{qos = Sub_QoS}) ->
    {Packets,Sub1} =
            take_all(ToTake,Q,Sub,
            [
                fun take_retained/3,
                fun take_queued/3
            ]),
    Packets1 = [P#packet{qos = min(P_QoS,Sub_QoS)} || P = #packet{qos = P_QoS} <- Packets],
    {Packets1,Sub1}.



take_retained(ToTake,_Q,Sub = #sub{idx = Idx,
                                   retained_iter = RetIter}) ->
    {Taken,RetRest} = take_gb_tree(ToTake,RetIter),
    {RetPs,Idx1} = enumerate(true,Idx,Taken),
    {RetPs,Sub#sub{retained_iter = RetRest,
                   idx = Idx1}}.

take_queued(ToTake,Q,Sub = #sub{idx = {_,QSeq} = Idx}) ->
    Taken = shared_queue:take_values(QSeq,ToTake,Q),
    {QPs,Idx1} = enumerate(false,Idx,Taken),
    {QPs,Sub#sub{idx = Idx1}}.

take_all(ToTake,Q,S = #sub{window = WSize},Generators) ->
    AccStart = {ToTake + WSize,[],Q,S},
    {RestWSize,Taken,_,S1} = lists:foldl(fun concat/2,AccStart,Generators),
    {Taken,S1#sub{window = RestWSize}}.

concat(Gen,{ToTakeAcc,TakenAcc,Q,S}) ->
    {Taken,S1} = Gen(ToTakeAcc,Q,S),
    {ToTakeAcc - length(Taken),TakenAcc ++ Taken,Q,S1}.

enumerate(IsRetained,Seq,Packets) ->
    IncFun = inc_fun(IsRetained,Seq),
    error_logger:info_msg("enumerating ~p~n",[Packets]),
    lists:mapfoldl(fun(P,Seq1) -> Seq2 = IncFun(Seq1), {P#packet{seq = Seq2 },Seq2} end,Seq,Packets).

inc_fun(true,{_,QSeq})    ->  fun({RetSeq1,_}) -> {RetSeq1 + 1,QSeq} end;
inc_fun(false,{RetSeq,_}) ->  fun({_,QSeq1})   -> {RetSeq,QSeq1 + 1} end.

%% @doc
%% Picking a subscription back up from where we left off
%% @end
iterator_from(undefined,Q,Sub) ->
    iterator_from({0,shared_queue:get_current_seq(Q)},Q,Sub);

iterator_from({RetSeq,QSeq},Q,Sub) ->
    ActualQSeq = max(QSeq,shared_queue:get_min_offset(Q)),
    resume_retained({RetSeq,ActualQSeq},Q,Sub).

take_gb_tree(_,none) -> {[],none};
take_gb_tree(Num,Iter) -> take_gb_tree1(Num,gb_trees:next(Iter),{[],Iter}).

take_gb_tree1(0,_,Acc) -> Acc;
take_gb_tree1(_,none,Acc) -> Acc;
take_gb_tree1(Take,{_,V,Iter1},{L,_}) -> take_gb_tree1(Take-1,gb_trees:next(Iter1),{[V|L],Iter1}).

%% @doc
%% Re-subscribing to existing subscription
%% @end
reset(QoS,Q,Sub = #sub{idx = {_,QSeq}}) ->
    Sub1 = Sub#sub{qos = QoS},
    resume_retained({0,QSeq},Q,Sub1).

resume_retained(Seq = {RetSeq,QSeq},Q,Sub) ->
    {_,Second} = shared_queue:split_by_seq(fun(ElSeq) -> ElSeq > QSeq end,Q),
    Tree = shared_queue:get_front_acc(Second),
    GbIter = gb_trees:iterator(Tree),
    {_,RetIter} = take_gb_tree(RetSeq,GbIter),
    {Seq, Sub#sub{retained_iter = RetIter,
                  idx = Seq}}.

new(CSeq,QoS,Q) ->
    new(CSeq,QoS,undefined,Q).

new(CSeq,QoS,From,Q) ->
    Sub = #sub{client_seq = CSeq,
                qos = QoS},
    iterator_from(From,Q,Sub).
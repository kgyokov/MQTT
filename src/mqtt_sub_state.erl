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
    resubscribe/3,
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
take(ToTake,Q,Sub) ->
        take_all(ToTake,Q,Sub,
        [
            fun take_retained/3,
            fun take_queued/3
        ]).


take_retained(ToTake,_Q,Sub = #sub{idx = {RetSeq,LastInQ},
                                   retained_iter = RetIter}) ->
    {Taken,RetRest} = gb_trees:take(ToTake,RetIter),
    RetPs = enumerate(true,{RetSeq,LastInQ},Taken),
    {RetPs,Sub#sub{retained_iter = RetRest,
                   idx = {RetSeq + length(Taken),LastInQ}}}.

take_queued(ToTake,Q,Sub = #sub{idx = {RetSeq,LastInQ}}) ->
    Taken = shared_queue:take(LastInQ,ToTake,Q),
    QPs = enumerate(false,{RetSeq,LastInQ},Taken),
    {QPs,Sub#sub{idx = {RetSeq,LastInQ + length(Taken)}}}.

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
    {EnumPs,_} = lists:mapfoldl(fun(P,Seq) -> {P#packet{retain = IsRetained, seq = Seq},IncFun(Seq)} end,Seq,Packets),
    EnumPs.

inc_fun(false,{_,QSeq}) ->
    fun({RetSeq1,_}) -> {RetSeq1 + 1,QSeq} end;

inc_fun(true,{RetSeq,_}) ->
    fun({_,QSeq1}) -> {RetSeq,QSeq1 + 1} end.

new(CSeq,QoS,Q) ->
    new(CSeq,QoS,undefined,Q).

new(CSeq,QoS,From,Q) ->
    Sub = #sub{client_seq = CSeq,
               qos = QoS},
    iterator_from(From,Q,Sub).

%% @doc
%% Picking a subscription back up from where we left off
%% @end
iterator_from(undefined,Q,Sub) ->
    iterator_from({0,shared_queue:max_offset(Q)},Q,Sub);

iterator_from({RetSeq,QSeq},Q,Sub) ->
    ActualQSeq = max(QSeq,shared_queue:min_offset(Q)),
    resume_retained({RetSeq,ActualQSeq},Q,Sub).

%% @doc
%% Re-subscribing to existing subscription
%% @end
resubscribe(QoS,Q,Sub = #sub{idx  = {_,QSeq}}) ->
    Sub1 = Sub#sub{qos = QoS},
    resume_retained({0,QSeq},Q,Sub1).

resume_retained(Seq = {RetSeq,QSeq},Q,Sub) ->
    {First,_} = shared_queue:split_by_seq(fun(Seq) -> Seq > QSeq end,Q),
    Tree = shared_queue:get_back_acc(First),
    RetIter = gb_trees:iterator_from(RetSeq,Tree),
    {Seq, Sub#sub{retained_iter = RetIter,
                  idx = Seq}}.

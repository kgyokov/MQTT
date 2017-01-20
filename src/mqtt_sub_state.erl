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
    new/5,
    new/6,
    resubscribe/4,
    is_old/2]).

-record(sub, {
    qos                                 ::qos(), %% QoS for this client subscription
    client_seq = 0                      ::non_neg_integer(), %% the version number of the client process registration
    %% (incremented every time a new client process is spawned, used to choose
    %% between different instances of a Client in case of race conditions)
    %% last_ack = 0  ::non_neg_integer(),   %% the filter-assigned sequence number of the last message processed by this client,
    last_in_q = 0                       ::non_neg_integer(),    %% the last message sent to the client
    window = 0                          ::non_neg_integer(),    %% How many messages the client has requested
    retained_msgs = versioned_set:new()    :: versioned_set:set(),                %% the retained messages to send to the client
    next_retained = 0                   ::non_neg_integer()    %% the retained message sequence for this subscription
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
        take_all(ToTake,Sub,
        [
            fun take_retained/2,
            fun(ToTake1,S1) -> take_queued(Q,ToTake1,S1) end
        ]).


take_retained(ToTake,Sub = #sub{last_in_q     = LastInQ,
                                retained_msgs = RetWaiting,
                                next_retained = RetSeq}) ->
    {Taken,RetRest} = versioned_set:take(ToTake,RetWaiting),
    RetPs = enumerate(true,{RetSeq,LastInQ},Taken),
    {RetPs,Sub#sub{retained_msgs = RetRest,
                   next_retained = RetSeq + length(Taken)}}.

take_queued(Q,ToTake,Sub = #sub{next_retained = RetSeq,
                                last_in_q     = LastInQ}) ->
    Taken = shared_queue:take(LastInQ,ToTake,Q),
    QPs = enumerate(false,{RetSeq,LastInQ},Taken),
    {QPs,Sub#sub{last_in_q = LastInQ + length(Taken)}}.

take_all(ToTake,S = #sub{window = WSize},Generators) ->
    TotalToTake = ToTake + WSize,
    {RestWSize,Taken,S1} = lists:foldl(fun concat/2,{TotalToTake,[],S},Generators),
    {Taken,S1#sub{window = RestWSize}}.

concat(Gen,{ToTakeAcc,TakenAcc,S}) ->
    {Taken,S1} = Gen(ToTakeAcc,S),
    {ToTakeAcc - length(Taken),TakenAcc ++ Taken,S1}.

enumerate(IsRetained,Seq,Packets) ->
    IncFun = inc_fun(IsRetained,Seq),
    {EnumPs,_} = lists:mapfoldl(fun(P,Seq) -> {P#packet{retain = IsRetained, seq = Seq},IncFun(Seq)} end,Seq,Packets),
    EnumPs.

inc_fun(false,{_,QSeq}) ->
    fun({RetSeq1,_}) -> {RetSeq1 + 1,QSeq} end;

inc_fun(true,{RetSeq,_}) ->
    fun({_,QSeq1}) -> {RetSeq,QSeq1 + 1} end.

new(CSeq,QoS,WSize,Q,Ret) ->
    new(CSeq,QoS,undefined,WSize,Q,Ret).

new(CSeq,QoS,From,WSize,Q,Ret) ->
    Sub = #sub{client_seq = CSeq,
               qos = QoS},
    iterator_from(From,WSize,Q,Ret,Sub).

%% @doc
%% Picking a subscription back up from where we left off
%% @end
iterator_from(undefined,WSize,Q,Ret,Sub) ->
    ResumeAt = {0,shared_queue:max_seq(Q)},
    iterator_from(ResumeAt,WSize,Q,Ret,Sub);

iterator_from({RetSeq,QSeq},WSize,Q,Ret,Sub) ->
    ActualQSeq = max(QSeq,shared_queue:min_seq(Q)),
    Sub1 = Sub#sub{last_in_q = ActualQSeq},
    Sub2 = resume_retained(RetSeq,ActualQSeq,Ret,Sub1),
    take(WSize,Q,Sub2).

%% @doc
%% Re-subscribing to existing subscription
%% @end
resubscribe(QoS,Q,Ret,Sub = #sub{last_in_q = QSeq}) ->
    Sub1 = Sub#sub{qos = QoS},
    Sub2 = resume_retained(0,QSeq,Ret,Sub1),
    take(0,Q,Sub2).

resume_retained(RetSeq,QSeq,Ret,Sub) ->
    RetainedMsgs = versioned_set:iterator_from(QSeq,RetSeq,Ret),
    Sub#sub{retained_msgs = RetainedMsgs,
            next_retained = RetSeq}.

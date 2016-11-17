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
    maybe_take/2,
    new/6,
    new/7,
    resubscribe/4,
    is_old/2]).

-record(sub, {
    qos                                 ::qos(), %% QoS for this client subscription
    pid                                 ::pid(), %% Process id of the process handling the client
    client_seq = 0                      ::non_neg_integer(), %% the version number of the client process registration
    %% (incremented every time a new client process is spawned, used to choose
    %% between different instances of a Client in case of race conditions)
    %% last_ack = 0  ::non_neg_integer(),   %% the filter-assigned sequence number of the last message processed by this client,
    next_in_q = 0                       ::non_neg_integer(),    %% the last message sent to the client
    window = 0                          ::non_neg_integer(),    %% How many messages the client has requested
    retained_msgs = shared_set:new()    ::shared_set:set(),                %% the retained messages to send to the client
    next_retained = 0                   ::non_neg_integer()    %% the retained message sequence for this subscription
}).

is_old(MsgCSeq,#sub{client_seq = CSeq}) when CSeq >= MsgCSeq -> true;
is_old(_,_) -> false.

maybe_take(Q,Sub) ->
    take(0,Q,Sub).

%% @doc
%% Determine the packets to send depending on the size of the Client's Window.
%% Called under one the following conditions:
%% - The Client has consumed one or more package and has increased its window size
%% - There may be new messages to send
%% @end
take(Num,Q,Sub = #sub{pid = Pid,window = LastWSize}) ->
    TotalToTake = Num + LastWSize,
    {RetPs,Sub1} = take_retained(TotalToTake,Sub),

    QToTake = TotalToTake - length(RetPs),
    {QPs,Sub2} = take_queued(QToTake,Q,Sub1),
    {Pid,RetPs ++ QPs,Sub2}.


take_retained(TotalToTake,Sub = #sub{next_in_q = NextInQ,
                                     retained_msgs = RetWaiting,
                                     next_retained = RetSeq}) ->
    {Taken,RetRest} = shared_set:take(TotalToTake,RetWaiting),
    NumTaken = length(Taken),
    RetPs = enumerate(true,NextInQ,RetSeq,Taken),
    {RetPs,Sub#sub{retained_msgs = RetRest,
                   next_retained = RetSeq + NumTaken,
                   window = TotalToTake - NumTaken}}.

take_queued(RestWSize,Q,Sub = #sub{retained_msgs = RetSeq,
                                   next_in_q = NextInQ}) ->
    Taken = shared_queue:take(NextInQ,NextInQ + RestWSize,Q),
    NumTaken = length(Taken),
    QPs = enumerate(false,NextInQ,RetSeq,Taken),
    {QPs,Sub#sub{next_in_q = NextInQ + NumTaken ,
                 window = RestWSize - NumTaken}}.

enumerate(IsRetained,QSeq,RetSeq,Packets) ->
    IncFun = inc_fun(IsRetained,RetSeq,QSeq),
    {EnumPs,_} = lists:mapfoldl(fun(P,Seq) -> {P#packet{retain = IsRetained, seq = Seq},IncFun(Seq)} end,RetSeq,Packets),
    EnumPs.

inc_fun(false,_,QSeq) ->
    fun({RetSeq1,_}) -> {RetSeq1 + 1,QSeq} end;

inc_fun(true,RetSeq,_) ->
    fun({_,QSeq1}) -> {RetSeq,QSeq1 + 1} end.


new(Pid,CSeq,QoS,WSize,Q,Ret) ->
    new(Pid,CSeq,QoS,undefined,WSize,Q,Ret).

new(Pid,CSeq,QoS,From,WSize,Q,Ret) ->
    Sub = #sub{pid = Pid,
               client_seq = CSeq,
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
    Sub1 = Sub#sub{next_in_q = ActualQSeq},
    Sub2 = resume_retained(RetSeq,ActualQSeq,Ret,Sub1),
    take(WSize,Q,Sub2).

%% @doc
%% Re-subscribing to existing subscription
%% @end
resubscribe(QoS,Q,Ret,Sub = #sub{next_in_q = QSeq}) ->
    Sub1 = Sub#sub{qos = QoS},
    Sub2 = resume_retained(0,QSeq,Ret,Sub1),
    take(0,Q,Sub2).

resume_retained(RetSeq,QSeq,Ret,Sub) ->
    RetainedMsgs = shared_set:iterator_from(QSeq,RetSeq,Ret),
    Sub#sub{retained_msgs = RetainedMsgs,
            next_retained = RetSeq}.

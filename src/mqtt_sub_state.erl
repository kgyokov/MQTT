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
    new/4,
    new/5,
    resubscribe/3,
    is_old/2]).

-record(sub, {
    qos                                 ::qos(), %% QoS for this client subscription
    client_seq = 0                      ::non_neg_integer(), %% the version number of the client process registration
    %% (incremented every time a new client process is spawned, used to choose
    %% between different instances of a Client in case of race conditions)
    %% last_ack = 0  ::non_neg_integer(),   %% the filter-assigned sequence number of the last message processed by this client,
    last_in_q = 0                       ::non_neg_integer(),    %% the last message sent to the client
    window = 0                          ::non_neg_integer(),    %% How many messages the client has requested
    retained_iter                       :: versioned_set:iter(),                %% the retained messages to send to the client
    last_retained = 0                   ::non_neg_integer()    %% the retained message sequence for this subscription
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


take_retained(ToTake,_Q,Sub = #sub{last_in_q     = LastInQ,
                                   retained_iter = RetIter,
                                   last_retained = RetSeq}) ->
    {Taken,RetRest} = versioned_set:take(ToTake,RetIter),
    RetPs = enumerate(true,{RetSeq,LastInQ},Taken),
    {RetPs,Sub#sub{retained_iter = RetRest,
                   last_retained = RetSeq + length(Taken)}}.

take_queued(ToTake,Q,Sub = #sub{last_retained = RetSeq,
                                last_in_q     = LastInQ}) ->
    Taken = shared_queue:take(LastInQ,ToTake,Q),
    QPs = enumerate(false,{RetSeq,LastInQ},Taken),
    {QPs,Sub#sub{last_in_q = LastInQ + length(Taken)}}.

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

new(CSeq,QoS,Q,Ret) ->
    new(CSeq,QoS,undefined,Q,Ret).

new(CSeq,QoS,From,Q,Ret) ->
    Sub = #sub{client_seq = CSeq,
               qos = QoS},
    iterator_from(From,Q,Ret,Sub).

%% @doc
%% Picking a subscription back up from where we left off
%% @end
iterator_from(undefined,Q,Ret,Sub) ->
    Seq = {0,shared_queue:max_seq(Q)},
    iterator_from(Seq,Q,Ret,Sub);

iterator_from({RetSeq,QSeq},Q,Ret,Sub) ->
    ActualQSeq = max(QSeq,shared_queue:min_seq(Q)),
    ActualSeq = {RetSeq,ActualQSeq},
    Sub1 = Sub#sub{last_in_q = ActualQSeq},
    resume_retained(ActualSeq,Ret,Sub1).

%% @doc
%% Re-subscribing to existing subscription
%% @end
resubscribe(QoS,Ret,Sub = #sub{last_in_q = QSeq}) ->
    Sub1 = Sub#sub{qos = QoS},
    Seq = {0,QSeq},
    resume_retained(Seq,Ret,Sub1).

resume_retained(Seq = {RetSeq,QSeq},Ret,Sub) ->
    RetIter = versioned_set:iterator_from(QSeq,RetSeq,Ret),
    {Seq, Sub#sub{retained_iter = RetIter,
                  last_retained = RetSeq}}.

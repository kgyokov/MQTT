%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Mar 2016 1:09 AM
%%%-------------------------------------------------------------------
-module(mqtt_sub_state).
-author("Kalin").

-include("mqtt_internal_msgs.hrl").

%% API
-export([maybe_update_waiting/2, take/3, resubscribe/3, new/7, resubscribe/5, resume/5]).

-record(sub, {
    qos             ::qos(), %% QoS for this client subscription
    monref          ::any(), %% monitor reference for the process handling the client
    pid             ::pid(), %% Process id of the process handling the client
    client_seq = 0  ::non_neg_integer(), %% the version number of the client process registration
    %% (incremented every time a new client process is spawned, used to choose
    %% between different instances of a Client in case of race conditions)
    %% last_ack = 0  ::non_neg_integer(),   %% the filter-assigned sequence number of the last message processed by this client,
    next_in_q = 0   ::non_neg_integer(),    %% the last message sent to the client
    window = 0      ::non_neg_integer(),    %% How many messages the client has requested
    retained_msgs = [] ::[],                %% the retained messages to send to the client
    next_retained = 0 ::non_neg_integer()    %% the retained message sequence for this subscription
}).

%% @doc
%% Updates a sub if the new packet sequence is within the window
%% @end
maybe_update_waiting(NextSeq,Sub = #sub{retained_msgs = RetWaiting,
                                        next_in_q = NextInQ,
                                        window = WSize})
                                        when NextSeq - NextInQ + length(RetWaiting) > 0, WSize > 0 ->
    NumToSend = min(NextInQ + WSize,NextSeq),
    {ok,Sub#sub{next_in_q = NextInQ + NumToSend,
                window = WSize - NumToSend}};

maybe_update_waiting(_,_) -> blocked.


%% @doc
%% Determine the sequence range of packets to send depending on the size of the Client's Window
%% @end

%% LastWSize should be 0
take(Num,Q,Sub = #sub{window = LastWSize}) ->
    TotalToTake = Num + LastWSize,
    {RetPs,Sub1} = take_retained(TotalToTake,Sub),

    QToTake = TotalToTake - length(RetPs),
    {QPs,Sub2} = take_shared(QToTake,Q,Sub1),
    {RetPs ++ QPs,Sub2}.


take_retained(TotalToSend,Sub = #sub{retained_msgs = RetWaiting,
                                     next_retained = NextRet}) ->
    NumRetToSend = min(TotalToSend,length(RetWaiting)),
    {RetToSend,RetRest} = lists:split(NumRetToSend,RetWaiting),
    {RetPs,_}   = enumerate(true,NextRet,RetToSend),
    {RetPs,Sub#sub{next_retained = NextRet + NumRetToSend,
                   retained_msgs = RetRest,
                   window = TotalToSend - NumRetToSend}}.

take_shared(RestWSize,Q,Sub = #sub{next_in_q = NextInQ}) ->
    QToSend = shared_queue:read(NextInQ,NextInQ + RestWSize,Q),
    NumQToSend = length(QToSend  ),
    {QPs,_}     = enumerate(false,NextInQ,QToSend),
    {QPs,Sub#sub{next_in_q = NextInQ + NumQToSend,
                 window = RestWSize - NumQToSend}}.


enumerate(IsRetained,StartSeq,Ps) ->
    Type =
        case IsRetained of
            true  -> ret;
            false ->   q
        end,
    {EnumPs,_} = lists:mapfoldl(fun(P,Seq) -> {P#packet{retain = IsRetained,ref = {Type,Seq}},Seq+1} end,StartSeq,Ps),
    EnumPs.

new(Pid,MonRef,ClientSeq,QoS,NextInQ,Ret,WSize) ->
    #sub{client_seq = ClientSeq,
         qos = QoS,
         pid = Pid,
         monref = MonRef,
         next_in_q = NextInQ, %% initialize the Sequence number of the next packet to send
         next_retained = 0,
         window = WSize,
         retained_msgs = gb_trees:values(Ret)}.

resume(Pid,MonRef,_ResumeFrom = {RetSeq,QSeq},WSize,Sub) ->
    Sub#sub{window = WSize,
            pid = Pid,
            next_in_q = QSeq,
            next_retained = RetSeq,
            monref = MonRef}.


%% @doc
%% Re-subscribing to existing subscription
%% @end
resubscribe(QoS,Ret,Sub = #sub{retained_msgs = RetWaiting,
                               next_retained = RetSeq}) ->
    Sub#sub{qos = QoS,
            %% We are essentially skipping the RetWaiting messages and flushing them out
            retained_msgs = gb_trees:values(Ret),
            next_retained = RetSeq + length(RetWaiting)}.

%% @doc
%% Re-subscribing to existing subscription
%% @end
resubscribe(Pid,MonRef,QoS,Ret,Sub) ->
    resubscribe(QoS,Ret,Sub#sub{pid = Pid,monref = MonRef}).








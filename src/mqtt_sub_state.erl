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
-export([maybe_update_waiting/2, take/3, ack_retained/2, refresh_sub/3, new/7]).

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
maybe_update_waiting(NextSeq,Sub = #sub{retained_msgs = {_,RetWaiting},
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
take(Num,SQ,Sub = #sub{window = LastWSize}) ->
    TotalToTake = Num + LastWSize,
    {RetPs,Sub1} = take_retained(TotalToTake,Sub),

    QToTake = TotalToTake - length(RetPs),
    {QPs,Sub2} = take_shared(QToTake,SQ,Sub1),
    {RetPs ++ QPs,Sub2}.


take_retained(TotalToSend,Sub = #sub{retained_msgs = {UnAcked,RetWaiting},
                                     next_retained = NextRet}) ->
    NumRetToSend = min(TotalToSend,length(RetWaiting)),
    {RetToSend,RetRest} = lists:split(NumRetToSend,RetWaiting),
    {RetPs,_}   = enumerate(ret,NextRet,RetToSend),
    {RetPs,Sub#sub{next_retained = NextRet + NumRetToSend,
                   retained_msgs = {UnAcked ++ RetToSend,RetRest},
                   window = TotalToSend - NumRetToSend}}.

take_shared(RestWSize,SQ,Sub = #sub{next_in_q = NextInQ}) ->
    SQToSend = shared_queue:read(NextInQ,NextInQ + RestWSize,SQ),
    NumQToSend = length(SQToSend ),
    {QPs,_}     = enumerate(q,NextInQ,SQToSend),
    {QPs,Sub#sub{next_in_q = NextInQ + NumQToSend,
                 window = RestWSize - NumQToSend}}.


enumerate(IsRetained,StartSeq,Ps) ->
    Type =
        case IsRetained of
            true  -> ret;
            false ->   q
        end,
    {EnumPs,_}= lists:mapfoldl(fun(P,Seq) -> {P#packet{retain = IsRetained,ref = {Type,Seq}},Seq+1} end,StartSeq,Ps),
    EnumPs.

%%next(_,Sub = #sub{next_retained = NextRetSeq,
%%                  retained_msgs = {Unacked,[NextRet|Rest]}}) ->
%%    {{retained,NextRet,NextRetSeq},
%%        Sub#sub{next_retained = NextRetSeq+1,
%%                retained_msgs = {[NextRet|Unacked],Rest}}};
%%
%%next(Max,Sub = #sub{next_in_q = NextInQ,
%%                    retained_msgs = {_,[]}}) when NextInQ =< Max ->
%%    {{queue,NextInQ},
%%        Sub#sub{next_in_q = NextInQ + 1}};
%%
%%next(Max,#sub{next_in_q = NextInQ,
%%              retained_msgs = {_,[]}}) when NextInQ > Max -> nil.

ack_retained(RetAck,Sub = #sub{retained_msgs = {UnAcked,RetWaiting},
                               next_retained = RetSeq}) ->
    UnackedSeq = RetSeq - length(UnAcked),
    if UnackedSeq =< RetAck, RetAck =< RetSeq ->
                {_,Unacked1} = lists:split(RetAck - RetSeq,UnAcked),
                Sub#sub{retained_msgs = {Unacked1,RetWaiting}};
       UnackedSeq =< RetSeq, RetSeq =< RetAck ->
                {_,RetWaiting1} = lists:split(RetAck - RetSeq,RetWaiting),
                Sub#sub{retained_msgs = {[],RetWaiting1}};
       RetSeq =< UnackedSeq, UnackedSeq =< RetAck ->
                Sub
    end.

new(ClientSeq,QoS,Pid,{NextInQ,NextRetained},WSize,MonRef,Ret) ->
    #sub{client_seq = ClientSeq,
         qos = QoS,
         pid = Pid,
         monref = MonRef,
         next_in_q = NextInQ, %% initialize the Sequence number of the next packet to send
         next_retained = NextRetained,
         window = WSize,
         retained_msgs = gb_trees:values(Ret)}.

refresh_sub(QoS,Ret,Sub = #sub{retained_msgs = {_,RetWaiting},
                               next_retained = RetSeq}) ->
    Sub#sub{qos = QoS,
            %% We are essentially skipping the RetWaiting messages and flushing them out
            retained_msgs = gb_trees:values(Ret),
            next_retained = RetSeq + length(RetWaiting)}.




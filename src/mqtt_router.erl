%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Routes incoming message to any established connections that should receive them
%%% @todo: Persist messages that do not match currently established connections
%%% @end
%%% Created : 16. Mar 2015 9:48 PM
%%%-------------------------------------------------------------------
-module(mqtt_router).
-author("Kalin").

-opaque client_reg() :: {client_id(),qos(),pid()}.

-define(BATCH_SIZE,20).

-include("mqtt_internal_msgs.hrl").

%% API
-export([
    global_route/1,
    fwd_message/2,
    call_msg_local/2,
    cast_msg_local/2,
    %%subscribe/6,
    unsubscribe/3,
    resume_sub/5,
    ack/3,
    pull/3, get_retained/1]).

-ifdef(TEST).
    -export([get_batches_to_send/2]).
-endif.


%% @doc
%% Takes a message and:
%%  - Finds subscribed clients
%%  - Finds Pids for connected clients
%%  - Sends messages to those Pids
%% @end

global_route(#mqtt_message{topic = Topic,
                           qos = MsgQoS,
                           retain = Retain,
                           content = Content} = M) ->
    Subs = get_matching_subs(Topic),
    Packet = #packet{topic = Topic,
                     content = Content,
                     qos = MsgQoS,
                     retain = Retain},
    mqtt_topic_repo:enqueue(Topic,M),
    error_logger:info_msg("Pushing message ~p to subs ~p~n",[M,Subs]),
    [mqtt_sub:push(Sub,Packet) || Sub <- Subs],
    ok.

%% Hiding mqtt_topic_repo for consistency
get_retained(Filters) ->
    [#packet{topic = Topic,
             retain = true,
             seq = Seq,
             content = Content,
             qos = QoS}||
            #mqtt_message{topic = Topic,
                          seq = Seq,
                          content = Content,
                          qos = QoS} <- mqtt_topic_repo:get_retained(Filters)].

ack(FromPid,ClientId,Seq) ->
    mqtt_sub:ack(FromPid,ClientId,Seq).

pull(FromPid,WSize,ClientId) ->
    mqtt_sub:pull(FromPid,WSize,ClientId).

cast_msg({Node,Regs},CTRPacket) ->
    rpc:cast(Node,?MODULE,cast_msg_local,[Regs,CTRPacket]).

cast_msg_local(Regs,CTRPacket) ->
    [mqtt_session_out:push_qos0(Pid,CTRPacket) || {_,_,Pid} <- Regs].

call_msg({Node,Regs},CTRPacket) ->
    Results = rpc:call(Node,?MODULE,call_msg_local,[Regs,CTRPacket]),
    [ case Result of
          {badrpc,Reason} -> {error,Reason,Regs};
          _ -> Result
      end
    || Result <- Results].

call_msg_local(Regs,CTRPacket) ->
    rpc:pmap({?MODULE,fwd_message},[CTRPacket],Regs).

fwd_message(Reg = {_,QoS,Pid},CTRPacket) ->
    %% @todo: use basic messaging (instead of depending on the specifics of gen_server:call)
    try mqtt_session_out:push_reliable(Pid,CTRPacket,QoS) of
        _ -> ok
    catch
        exit:{noproc, _} -> {noproc,Reg}
    end.

%%%===================================================================
%%% Wrap mqtt_sub and mqtt_sub_repo interaction
%%%===================================================================

%%%% @doc
%%%% Get the Pids of connected clients matching the topic
%%%%
%%%% @end
%%-spec get_registered_clients(binary()) ->
%%    [client_reg()].
%%get_registered_clients(Topic) ->
%%    lists:flatten(
%%        [mqtt_sub:get_live_clients(Sub) || Sub <- get_matching_subs(Topic)]
%%    ).

unsubscribe(Filter,ClientId,Seq) ->
    Pid = get_sub(Filter),
    mqtt_sub:cancel(Pid,ClientId,Seq).

resume_sub(SubPid,ClientId,CSeq,{Filter,QoS,undefined},WSize) ->
    Pid = get_sub(Filter),
    {ok,ResumingFrom} = mqtt_sub:subscribe_self(Pid,SubPid,ClientId,CSeq,QoS,WSize),
    {ok,ResumingFrom,monitor(process,Pid)};

resume_sub(SubPid,ClientId,CSeq,Sub = {Filter,QoS,From},WSize) ->
     error_logger:info_msg("Now Resuming sub ~p~n",[Sub]),
     Pid = get_sub(Filter),
     {ok,ResumingFrom} = mqtt_sub:resume(Pid,SubPid,ClientId,CSeq,QoS,From,WSize),
     {ok,ResumingFrom,monitor(process,Pid)}.

%%resume_sub(ClientId,CSeq,Sub = {Filter,QoS}) ->
%%    [begin
%%         Pid = get_sub(Filter),
%%         mqtt_sub:resume(Pid,ClientId,CSeq,QoS,),
%%         Pid
%%     end || {Filter,QoS} <- Subs].


%% ========================================================================
%% Private functions - side effect free
%% ========================================================================

get_batches_to_send(Regs,MsgQoS) ->
    Regs1 = dedup_registered_clients(Regs),
    Regs2 = [{ClientId,min(SubQoS,MsgQoS),Pid} || {ClientId,SubQoS,Pid} <- Regs1],
    {QoS_0,QoS_Rel} = lists:partition(fun({_,QoS,_}) -> QoS =:= ?QOS_0 end,Regs2),
    {batch_per_node(QoS_0), batch_per_node(QoS_Rel)}.

dedup_registered_clients(Regs) ->
    Dedups = highest_qos_per_client(Regs),
    [{ClientId,QoS,Pid}|| {ClientId,{QoS,Pid}} <- dict:to_list(Dedups)].

highest_qos_per_client(Regs) ->
    lists:foldl(fun({ClientId,QoS,Pid}, D) ->
        dict:update(ClientId,
            fun ({QoS_Old,_})       when QoS_Old < QoS -> {QoS,Pid};
                (Old = {QoS_Old,_}) when QoS_Old >= QoS -> Old
            end,
            {QoS,Pid},D) end,
        dict:new(), Regs).

batch_per_node(ClientRegs) ->
    RegsPerNode = group_by_node(ClientRegs),
    lists:flatmap(fun({Node,NodeRegs}) ->
        [{Node,Batch} || Batch <- split_into_batches(?BATCH_SIZE,NodeRegs)]
    end,
        RegsPerNode).


%% @todo: Move to a utility module/library
split_into_batches(Len,L) ->
    split_into_batches(Len,L,[]).

split_into_batches(Len,L,B) when length(L) > Len ->
    {H,T} = lists:split(Len,L),
    split_into_batches(Len,T,[H|B]);

split_into_batches(Len,L,B) when length(L) =< Len ->
    [L|B].

group_by_node(Regs) ->
    NodeRegs = [{node(Pid), Reg} || Reg = {_,_,Pid} <- Regs],
    Groups = lists:foldl(fun({K,V}, D) -> dict:append(K, V, D) end, dict:new(), NodeRegs),
    dict:to_list(Groups).



%% ========================================================================
%% Private functions
%% ========================================================================

get_sub(Filter) ->
    case mqtt_sub_repo:get_filter_claim(Filter) of
        {ok,Pid} -> maybe_create_new_sub(Filter,Pid);
        error  -> create_new_sub(Filter)
    end.

get_matching_subs(Topic) ->
    [ maybe_create_new_sub(Filter,Sub)
        || {Filter,Sub} <- mqtt_sub_repo:get_matching_subs(Topic)].

maybe_create_new_sub(Filter,Pid) ->
    case is_pid(Pid) andalso rpc:pinfo(Pid,status) =/= undefined of
        true -> Pid;
        false -> create_new_sub(Filter)
    end.

create_new_sub(Filter) ->
    error_logger:info_msg("Creating new sub for ~p~n", [Filter]),
    {ok,Pid} = mqtt_sub:new(Filter),
    Pid.
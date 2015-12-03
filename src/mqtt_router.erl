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
-export([global_route/1, fwd_message/2, call_msg_local/2, cast_msg_local/2, subscribe/4, unsubscribe/3, refresh_subs/3]).

%% @doc
%% Takes a message and:
%%  - Finds subscribed clients
%%  - Finds Pids for connected clients
%%  - Sends messages to those Pids
%% @end
global_route(Msg = #mqtt_message{topic = Topic,qos = MsgQoS,
                                 content = Content,seq = Seq}) ->
    mqtt_topic_repo:enqueue(Topic,Msg),
    Regs = get_client_regs(Topic),
    {QoS_0_PerNode,QoS_Rel_PerNode} = split_into_node_batches(Regs,MsgQoS),
    %% @todo: determine min QoS

    CTRPacket = {Topic,Content,Seq},
    %% Send out QoS messages, do NOT wait for response
    [cast_msg(NodeRegs ,CTRPacket) || NodeRegs <- QoS_0_PerNode],
    %% Send out QoS 1/2 messages to registered processes and wait for response
    SyncResults = lists:flatten([call_msg(NodeRegs ,CTRPacket) || NodeRegs  <- QoS_Rel_PerNode]),

    %% Handle results
    %% @todo: Fault tolerance
    FailedRegs = [Reg || {error,_Reason,Reg} <- SyncResults],
    case FailedRegs of
        []  -> ok;
        _   -> error({failed_delivery,FailedRegs})
    end.


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

%% @doc
%% Get the Pids of connected clients matching the topic
%%
%% @end
-spec get_client_regs(binary()) ->
    [client_reg()].
get_client_regs(Topic) ->
    Regs = [mqtt_sub:get_live_clients(Sub) || Sub <- get_matching_subs(Topic)],
    Regs1 = lists:flatten(Regs),
    Dedups = highest_qos_per_client(Regs1),
    [{ClientId,QoS,Pid}|| {ClientId,{QoS,Pid}} <- dict:to_list(Dedups)].

highest_qos_per_client(Regs) ->
    lists:foldr(fun({ClientId,QoS,Pid}, D) ->
        dict:update(ClientId,
            fun ({QoS_Old,_})       when QoS_Old < QoS -> {QoS,Pid};
                (Old = {QoS_Old,_}) when QoS_Old >= QoS -> Old
            end,
            {QoS,Pid},D) end,
        dict:new(), Regs).


subscribe(Filter,ClientId,QoS,Seq) ->
    Pid = get_sub(Filter),
    mqtt_sub:subscribe_self(Pid,ClientId,QoS,Seq).


unsubscribe(Filter,ClientId,Seq) ->
    Pid = get_sub(Filter),
    mqtt_sub:unsubscribe(Pid,ClientId,Seq).

refresh_subs(ClientId,Seq,Subs) ->
    [begin
         Pid = get_sub(Filter),
         mqtt_sub:subscribe_self(Pid,ClientId,QoS,Seq),
         Pid
     end || {Filter,QoS} <- Subs].


%% ========================================================================
%% Private functions -
%% ========================================================================

split_into_node_batches(Regs,MsgQoS) ->
    %% @todo: determine min QoS
    RegsWQoS = [{ClientId,min(SubQoS,MsgQoS),Pid} || {ClientId,SubQoS,Pid} <- Regs],
    {QoS_0,QoS_Rel} = lists:partition(fun({_,QoS,_}) -> QoS =:= ?QOS_0 end,RegsWQoS),
    {batch_up(QoS_0),batch_up(QoS_Rel)}.

batch_up(ClientRegs) ->
    RegsPerNode = group_by_node(ClientRegs),
    lists:flatmap(fun({Node,NodeRegs}) ->
        [{Node,Batch} || Batch <-split_into_batches(?BATCH_SIZE,NodeRegs)]
    end,
        RegsPerNode).

split_into_batches(Len,L) ->
    split_into_batches(Len,L,[]).

split_into_batches(Len,L,B) when length(L) > Len ->
    {H,T} = lists:split(Len,L),
    split_into_batches(Len,T,[H|B]);

split_into_batches(Len,L,B) when length(L) =< Len ->
    [L|B].

group_by_node(Regs) ->
    NodeRegs = [{node(Pid), Reg} || Reg = {_,_,Pid} <- Regs],
    Groups = lists:foldr(fun({K,V}, D) -> dict:append(K, V, D) end, dict:new(), NodeRegs),
    dict:to_list(Groups).



%% ========================================================================
%% Private functions
%% ========================================================================

get_sub(Filter) ->
    case mqtt_sub_repo:get_sub(Filter) of
        {ok,Pid} -> maybe_create_new_sub(Filter,Pid);
        error  -> create_new_sub(Filter)
    end.

get_matching_subs(Topic) ->
    [ maybe_create_new_sub(Filter,Sub)
        || {Filter,Sub} <- mqtt_sub_repo:get_matching_subs(Topic)].

maybe_create_new_sub(Filter,Pid) ->
    case is_pid(Pid) andalso is_process_alive(Pid) of
        true -> Pid;
        false -> create_new_sub(Filter)
    end.

create_new_sub(Filter) ->
    error_logger:info_msg("Crearing new sub for ~p~n", [Filter]),
    {ok,Pid} = mqtt_sub:new(Filter),
    Pid.
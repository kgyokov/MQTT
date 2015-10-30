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

-define(BATCH_SIZE,20).

-include("mqtt_internal_msgs.hrl").

%% API
-export([global_route/1, fwd_message/2]).


global_route(Msg = #mqtt_message{topic = Topic,qos = MsgQoS,
                                 content = Content,seq = Seq}) ->
    mqtt_topic_repo:enqueue(Topic,Msg),
    Regs = get_client_regs(Topic),

    {QoS_0,Live,Dead} = split_regs_by_state(Regs,MsgQoS),

    error_logger:info_msg("To enqueue ~p for topic ~p",[Msg,Topic]),
    CTRPacket = {Topic,Content,Seq},
    % Send out QoS messages, do NOT wait for response
    [cast_msg(NodeRegs ,CTRPacket) || NodeRegs <- QoS_0],
    %% Send out QoS 1/2 messages to registered processes and wait for response
    SyncResults = lists:flatten([call_msg(NodeRegs ,CTRPacket) || NodeRegs  <- Live]),

    %% Handle results
    %% @todo: Fault tolerance
    FailedRegs = [Reg || {error,_Reason,Reg} <- SyncResults],
    case FailedRegs of
        []  -> ok;
        _   -> error({failed_delivery,FailedRegs})
    end,
    StaleRegs = [Tuple || {noproc,Tuple} <- SyncResults],
    %% Get rid of any stale process registrations
    lists:map(fun({Pid,ClientId,_}) -> mqtt_reg_repo:unregister(Pid,ClientId) end,
              StaleRegs),

    %% persist if there are any subscribed clients that did not receive the message
    case {StaleRegs,Dead} of
        {[],[]} -> ok;
        _       -> persist_message(CTRPacket)
    end.


-spec get_client_regs(binary()) ->
    [{ClientId::binary(),
      SubQos :: qos(),
      Reg :: {ok,Pid::pid()} | undefined}].

%% Get the Pids of connected clients matching the topic
get_client_regs(Topic) ->
    [begin
        Reg = mqtt_reg_repo:get_registration(ClientId),
        {ClientId,SubQoS,Reg}
     end
        || {ClientId,SubQoS} <- mqtt_sub_repo:get_matches(Topic)].

split_regs_by_state(Regs,MsgQoS) ->
    RegStates = lists:filtermap(fun({ClientId,SubQoS,Reg}) ->
        QoS = min(MsgQoS,SubQoS),
        case Reg of
            {ok,Pid}  ->
                {true,{live,{Pid,ClientId},QoS}};
            undefined when QoS =/= ?QOS_0 ->
                {true,{dead, ClientId, QoS}};
            undefined when QoS =:= ?QOS_0 ->
                false
        end
    end,Regs),

    {QoS_0,QoS_Reliable} = lists:partition(fun({_,_,QoS}) -> QoS =:= ?QOS_0 end,RegStates),
    {Live,Dead} = lists:partition(fun({State,_,_QoS}) -> State =:= live end, QoS_Reliable),

    QoS_0_PerNode = batch_up(QoS_0),
    Live_PerNode = batch_up(Live),
    {QoS_0_PerNode,Live_PerNode,Dead}.

batch_up(Regs) ->
    RegsPerNode = group_by_node(Regs),
    lists:flatmap(fun({Node,NodeRegs}) ->
                    Batches = split_into_batches(NodeRegs,?BATCH_SIZE),
                    lists:map(fun(Batch) -> {Node,Batch} end,Batches)
                  end,
        RegsPerNode).

split_into_batches(L,Len) ->
    split_into_batches(L,Len,[]).

split_into_batches(L,Len,B) when length(L) > Len ->
    {H,T} = lists:split(L,Len),
    split_into_batches(T,Len,[H|B]);

split_into_batches(L,Len,B) when length(L) =< Len ->
    [L|B].

group_by_node(Regs) ->
    NodeRegs = [{node(Pid), Reg} || Reg = {Pid,_,_} <- Regs],
    Groups = lists:foldr(fun({K,V}, D) -> dict:append(K, V, D) end, dict:new(), NodeRegs),
    dict:to_list(Groups).

cast_msg({Node,Regs},CTRPacket) ->
    rpc:cast(Node,?MODULE,cast_msg_local,[Regs,CTRPacket]).

cast_msg_local(Regs,CTRPacket) ->
    [mqtt_session_out:push_qos0(Pid,CTRPacket) || {Pid,_,_} <- Regs].

call_msg({Node,Regs},CTRPacket) ->
    Results = rpc:call(Node,?MODULE,call_msg_local,[Regs,CTRPacket]),
    [ case Result of
          {badrpc,Reason} -> {error,Reason,Regs};
          _ -> Result
      end
    || Result <- Results].

call_msg_local(Regs,CTRPacket) ->
    rpc:pmap({?MODULE,fwd_message},[CTRPacket],Regs).

fwd_message(Reg = {Pid,_,QoS},CTRPacket) ->
    %% @todo: use basic messaging (instead of depending on the specifics of gen_server:call)
    try mqtt_session_out:push_reliable(Pid,CTRPacket,QoS) of
        _ -> ok
    catch
        exit:{noproc, _} -> {noproc,Reg}
    end.

persist_message(_CTRPacket) ->
    ok.


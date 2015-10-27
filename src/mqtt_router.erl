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

-include("mqtt_internal_msgs.hrl").

%% API
-export([global_route/1, fwd_message/2]).


global_route(Msg = #mqtt_message{topic = Topic,qos = MsgQoS,
                                 content = Content,seq = Seq}) ->
    mqtt_topic_repo:enqueue(Topic,Msg),
    Regs = get_client_regs(Topic),

    {QoS_0,MaybeLive,Dead} = get_regs_by_state(Regs,MsgQoS),

    error_logger:info_msg("To enqueue ~p for topic ~p",[Msg,Topic]),
    CTRPacket = {Topic,Content,Seq},
    % Send out QoS messages, do not wait for response
    [mqtt_session_out:push_qos0(Pid,CTRPacket) || {_,{Pid,_},_QoS} <- QoS_0],
    %% Send out QoS 1/2 messages to registered processes and wait for response
    SyncResults = rpc:pmap({?MODULE,fwd_message},[CTRPacket],MaybeLive),

    %% Handle results
    StaleRegs = [Tuple || {noproc,Tuple} <- SyncResults],
    %% Get rid of any stale process registrations
    lists:map(fun({Pid,ClientId,_}) -> mqtt_reg_repo:unregister(Pid,ClientId) end,
              StaleRegs),
    %% persist if there are registrations
    case {StaleRegs,Dead} of
        {[],[]} -> ok;
        _       -> persist_message(CTRPacket)
    end.

get_client_regs(Topic) ->
    [begin
        Reg = mqtt_reg_repo:get_registration(ClientId),
        {ClientId,SubQoS,Reg}
     end
        || {ClientId,SubQoS} <- mqtt_sub_repo:get_matches(Topic)].

get_regs_by_state(Regs,MsgQoS) ->
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
    {MaybeLive,Dead} = lists:partition(fun({State,_,_QoS}) -> State =:= live end, QoS_Reliable),
    {QoS_0,MaybeLive,Dead}.

fwd_message({live,{Pid,ClientId},QoS},CTRPacket) ->
    %% @todo: use basic messaging (instead of depending on the specifics of gen_server:call)
    try mqtt_session_out:push_reliable(Pid,CTRPacket,QoS) of
        _ -> ok
    catch
        exit:{noproc, _} -> {noproc,{Pid,ClientId,QoS}}
    end.

persist_message(_CTRPacket) ->
    ok.


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
    Subs = mqtt_sub_repo:get_matches(Topic),

    CTRPacket = {Topic,Content,Seq},
    Regs = lists:filtermap(fun({ClientId,SubQoS}) ->
                QoS = min(MsgQoS,SubQoS),
                case mqtt_reg_repo:get_registration(ClientId) of
                    {ok,Pid}  ->
                        {true,{live,{Pid,ClientId},QoS}};
                    undefined when QoS =/= ?QOS_0 ->
                        {true,{dead, ClientId, QoS}};
                    undefined when QoS =:= ?QOS_0 ->
                        false
                end
         end,Subs),
    error_logger:info_msg("To enqueue ~p for topic ~p",[Msg,Topic]),
    {QoS_0,QoS_Reliable} = lists:partition(fun({_,_,QoS}) -> QoS =:= ?QOS_0 end,Regs),
    {MaybeLive,Dead} = lists:partition(fun({State,_,_QoS}) -> State =:= live end, QoS_Reliable),

    % Send out QoS messages, do not wait for response
    [mqtt_session_out:push_qos0(Pid,CTRPacket) || {_,{Pid,_},_QoS} <- QoS_0],
    %% Send out QoS 1/2 messages to registered processes and wait for response
    SyncResults = rpc:pmap({?MODULE,fwd_message},[CTRPacket],MaybeLive),
    StaleRegs = [Tuple || {noproc,Tuple} <- SyncResults],
    %% Get rid of any stale process registrations
    lists:map(fun({Pid,ClientId,_}) -> mqtt_reg_repo:unregister(Pid,ClientId) end,
              StaleRegs),
    %% persist if necessary
    case {StaleRegs,Dead} of
        {[],[]} -> ok;
        _       -> persist_message(CTRPacket)
    end.


fwd_message({live,{Pid,ClientId},QoS},CTRPacket) ->
    %% @todo: use basic messaging (instead of depending on the specifics of gen_server:call)
    try mqtt_session_out:push_reliable(Pid,CTRPacket,QoS) of
        _ -> ok
    catch
        exit:{noproc, _} -> {noproc,{Pid,ClientId,QoS}}
    end.

persist_message(_CTRPacket) ->
    ok.


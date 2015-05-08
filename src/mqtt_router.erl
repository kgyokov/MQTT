%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
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
    CTRPacket = {Topic,Content,Seq},
    Subs = mqtt_sub_repo:get_matches(Topic),
    Regs = lists:map(fun({ClientId,SubQoS}) ->
                Qos = min(MsgQoS,SubQoS),
                case mqtt_reg_repo:get_registration(ClientId) of
                    {ok,Pid}  -> {live,{Pid,ClientId},Qos};
                    undefined -> {dead, ClientId, Qos}
                end
         end,Subs),

    error_logger:info_msg("To enqueue ~p for topic ~p",[Msg,Topic]),
    mqtt_topic_repo:enqueue(Topic,Msg),
    {QoS_0,QoS_Reliable} = lists:partition(fun({_,_,QoS}) -> QoS =:= ?QOS_0 end,Regs),

    % Send out QoS messages, do not wwait for response
    [begin
         {Pid,_}=  Pair,
         mqtt_session_out:push_qos0(Pid,CTRPacket)
     end
        || {State,Pair,_QoS} <- QoS_0, State =:= live],

    {MaybeLive,Dead} = lists:partition(fun({State,_,_QoS}) -> State =:= live end, QoS_Reliable),
    %% Send out QoS 1/2 messages to registered processes and wait for response
    SyncResults = rpc:pmap({?MODULE,fwd_message},[CTRPacket],MaybeLive),
%%     OldRegs = lists:filtermap(fun(Result) ->
%%                             case Result of
%%                                 ok                  ->  false;
%%                                 {noproc,Tuple} ->  {true,Tuple}
%%                             end
%%                            end, SyncResults),
    OldRegs =
    [begin
          {noproc,Tuple} = Result,
          Tuple
     end || Result <- SyncResults, Result =/= ok],

    %% Get rid of any stale process registrations
    lists:map(fun({Pid,ClientId,_}) -> mqtt_reg_repo:unregister(Pid,ClientId) end,
              OldRegs),
    %% persist if necessary
    case {OldRegs,Dead} of
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


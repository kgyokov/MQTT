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

-include("mqtt_session.hrl").

%% API
-export([global_route/1, fwd_message/2]).


global_route(#mqtt_message{topic = Topic,retain = Retain,
                           dup = Dup,qos = MsgQoS,
                           content = Content, seq = Ref}) ->
    CTRPacket = {Topic,Content,Retain,Dup,Ref},
    List =
    [
%%         begin
%%             case mqtt_reg_repo:get_registration(ClientId) of
%%                 {ok,ConnPid} ->
%%                     QoS = min(MsgQoS,SubQoS),
%%                     fwd_msg(ConnPid,{Topic,Content,Retain,QoS,Ref});
%%                 _            -> ok
%%             end
%%         end
        {ConnPid,min(MsgQoS,SubQoS)}
        || {ClientId,SubQoS} <- mqtt_sub_repo:get_matches(Topic),
           {ok,ConnPid} = mqtt_reg_repo:get_registration(ClientId)
    ],
    {QoS_0,QoS_Reliable} = lists:partition(fun({_,QoS}) -> QoS =:= 0 end,List),
    [ mqtt_session_out:push_qos0(ConnPid,CTRPacket) || {ConnPid,_} <- QoS_0 ],
    rpc:pmap({?MODULE,fwd_message},[CTRPacket],QoS_Reliable).

fwd_message({ConnPid,QoS},CTRPacket) ->
    mqtt_session_out:push_reliable(ConnPid,CTRPacket,QoS).


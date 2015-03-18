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
-export([global_route/1]).


global_route(#mqtt_message{topic = Topic,
                                 retain = Retain,dup = Dup,qos = MsgQoS,content = Content}) ->
    Ref = make_ref(),
    [
        begin
            case mqtt_reg_repo:get_registration(ClientId) of
                {ok,ConnPid} ->
                    QoS = min(MsgQoS,SubQoS),
                    fwd_msg(ConnPid,{Topic,Content,Retain,QoS,Ref});
                _            -> ok
            end
        end
    || {ClientId,SubQoS} <- mqtt_sub_repo:get_matches(Topic)
].

fwd_msg(SessPid,{_Topic,_Content,_Retain,_QoS,Ref}) ->
    mqtt_session_out:append_msg(SessPid,{_Topic,_Content,_Retain,_QoS},Ref).

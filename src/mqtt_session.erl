%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Feb 2015 5:37 PM
%%%-------------------------------------------------------------------
-module(mqtt_session).
-author("Kalin").

%%-include("mqtt_packets.hrl").
-include("mqtt_internal_msgs.hrl").
%% API
-export([append_msg/3,
         append_message_comp/2,
         message_ack/2,
         message_pub_rec/2,
         message_pub_comp/2,
         %%recover/1,
         subscribe/2,
         unsubscribe/2,
         cleanup/1,
         new/2, to_publish/4, to_pubrel/1]).


-record(session_out,{
    client_id                 ::binary(),            %% The id of the client
    is_persistent             ::boolean(),           %% whether the session needs to be persisted
    packet_seq                ::non_neg_integer(),   %% The latest packet id (incremented by 1 for every packet)
    qos1 = dict:new()         ,
    qos2 = dict:new()         ,
    qos2_rec = gb_sets:new()  ,
    refs = gb_sets:new()      ,
    subs = orddict:new()
%%     min_subs = orddict:new()
}).


%% ================================================================================
%% SUBSCRIPTIONS
%% ================================================================================

subscribe(S =#session_out{subs = Subs, client_id = ClientId},NewSubs) ->

    %% Maintain a flat list of subscriptions
    %% @todo: Optimize/deduplicate
    Subs1 = lists:foldr(fun({Topic,QoS},Acc) ->
                                orddict:store(Topic,QoS,Acc)
                              end,
                             Subs, NewSubs),
    [
        begin
            error_logger:info_msg("Subscribing: ~p,~p,~p,~n",[ClientId,Topic,QoS]),
            mqtt_sub_repo:add_sub(ClientId,Topic,QoS)
        end
        || {Topic,QoS} <- NewSubs
    ],

    %% @todo: Deduplicate, persist
    S#session_out{subs = Subs1}.

unsubscribe(S = #session_out{subs = Subs, client_id = ClientId},OldSubs) ->
    [
        mqtt_sub_repo:remove_sub(ClientId,Topic)
        || Topic <- OldSubs
    ],
    S#session_out{subs =
                  lists:foldr(fun(Topic,Acc) ->
                                orddict:erase(Topic,Acc)
                              end,
                              Subs, OldSubs)}.

%% get_sub_diff(Subs,NewSubs) ->
%%     orddict:update().

%% check_sub_exists(Subs,NewSub = {Filter,QoS}) ->
%%     case lists:dropwhile(fun(Sub) -> not mqtt_topic:is_covered_by(Sub, NewSub) end, Subs) of
%%         [] -> error;
%%         [Sub| _] = Sub
%%     end


%% clear persisted data during shutdown
cleanup(S = #session_out{client_id = ClientId}) ->
    mqtt_reg_repo:unregister(ClientId),
    maybe_clear_subs(S).

maybe_clear_subs(#session_out{subs = Subs, client_id = ClientId, is_persistent = true}) ->
    [ mqtt_sub_repo:remove_sub(ClientId,Topic) || {Topic,_QoS}  <- Subs ],
    ok;

maybe_clear_subs(_S) ->
    ok.



%%add_or_replace_sub(ClientId,{Topic,QoS},Subs)->
%%     State = case orddict:find(Topic,Subs) of
%%                 error ->
%%                     new;
%%                 {ok,CurrentQoS} when CurrentQoS =/= QoS ->
%%                     replaced;
%%                 {ok,CurrentQoS} when CurrentQoS =:= QoS ->
%%                     exists
%%             end,
%%     mqtt_sub_repo:add_sub(ClientId,Topic,QoS),
%%     {State,orddict:store(Topic,QoS,Subs)}.


%% =========================================================================
%% MESSAGES
%% =========================================================================

%% @doc
%% Appends message for delivery
%% @end
append_msg(Session,CTRPacket = {_Topic,_Content,_Retain,_Dup,Ref},QoS) ->
    #session_out{refs = Refs, subs = _Subs} = Session,

    case gb_sets:is_member(Ref,Refs) of
        false -> forward_msg(Session,CTRPacket,QoS);
        true  -> duplicate
    end.

forward_msg(Session,CTRPacket = {_Content,_Topic,_Retain,_Dup,Ref},QoS) ->
    #session_out{packet_seq = PacketSeq, refs = Refs} = Session,

    NewPacketSeq = PacketSeq+1,
    PacketId = if QoS =:= ?QOS_1;
                  QoS =:= ?QOS_2 ->
                        NewPacketSeq band 16#ffff;
                  true ->
                        undefined
               end,
    Session1 = Session#session_out{refs = gb_sets:add(Ref,Refs)},
    NewSession = (case QoS of
                      ?QOS_1 ->
                          #session_out{qos1 = QosQueue} = Session1,
                          Session1#session_out{qos1 = orddict:store(PacketId,CTRPacket,QosQueue),
                                               packet_seq = NewPacketSeq};
                      ?QOS_2 ->
                          #session_out{qos2 = QosQueue} = Session1,
                          Session1#session_out{qos2 = orddict:store(PacketId,CTRPacket,QosQueue),
                                               packet_seq = NewPacketSeq};
                      ?QOS_0 ->
                          Session
                  end),
%%       #session_out{refs = gb_sets:add(Ref,Refs)},
    {ok,NewSession,PacketId}.

append_message_comp(Session = #session_out{refs = Refs}, Ref) ->
    Session#session_out{refs = gb_sets:del_element(Ref,Refs)}.


message_ack(Session,PacketId) ->
    #session_out{qos1 = Msgs} = Session,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            {ok,
             Session#session_out{qos1 = orddict:erase(PacketId,Msgs)}};
        error ->
            duplicate
    end.


message_pub_rec(Session,PacketId) ->
    #session_out{qos2 = Msgs, qos2_rec = Ack} = Session,
    case orddict:find(PacketId,Msgs) of
        {ok,_} ->
            {ok,
             Session#session_out{qos2 = orddict:erase(PacketId,Msgs),
                                 qos2_rec = gb_sets:add(PacketId,Ack)}};
        error ->
            duplicate
    end.


message_pub_comp(Session,PacketId)  ->
    #session_out{qos2_rec = Ack} = Session,
    case gb_sets:is_member(PacketId,Ack) of
        true ->
            {ok,
            Session#session_out{qos2_rec = gb_sets:delete(PacketId,Ack)}};
        false ->
            duplicate
    end.



%% =========================================================================
%% RECOVERY
%% =========================================================================

%% recover(Session) ->
%%     recover_in_flight(Session),
%%     recover_queued(Session).
%%
%% recover_in_flight(#session_out{qos1 = UnAck1, qos2 = UnAck2,
%%                                qos2_rec = Rec, packet_seq = PacketSeq}) ->
%%     NewPackets =
%%     [ to_publish(CTRPacket,?QOS_1,PacketId, true) || {PacketId,CTRPacket}  <- orddict:to_list(UnAck1)] ++
%%     [ to_publish(CTRPacket,?QOS_2,PacketId, true)  || {PacketId,CTRPacket}  <- orddict:to_list(UnAck2)] ++
%%     [ to_pubrel(PacketId) || {PacketId,PacketId}  <- gb_sets:to_list(Rec)],
%%     {PacketSeq,NewPackets}.
%%
%% recover_queued(_Session) ->
%%     ok.
%%
%% get_retained(_Session) ->
%%     ok.

to_publish({Topic,Content,Retain,_Dup,_Ref}, QoS, PacketId, Dup) ->
    #'PUBLISH'{content = Content,packet_id = PacketId,
               qos = QoS,topic = Topic,
               dup = Dup,retain = Retain}.

to_pubrel(PacketId) ->
    #'PUBREL'{packet_id = PacketId}.


new(ClientId,CleanSession) ->
    #session_out{
        client_id = ClientId,
        is_persistent = not CleanSession,
        packet_seq = 0,
        qos1 = orddict:new(),
        qos2 = orddict:new(),
        qos2_rec = gb_sets:new(),
        refs = gb_sets:new(),
        subs = orddict:new()
    }.


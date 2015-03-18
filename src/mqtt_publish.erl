%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Dec 2014 8:13 PM
%%%-------------------------------------------------------------------
-module(mqtt_publish).
-author("Kalin").

-include("mqtt_pubsub.hrl").
-include("mqtt_session.hrl").

%% API
-export([at_most_once/2, at_least_once/2, exactly_once_phase1/2, exactly_once_phase2/2, recover/1, discard_will/1]).

discard_will(Session) ->
    NewSession = Session#session_in{will = undefined},
    maybe_persist(NewSession).

at_most_once(Msg,Session) ->
    fwd_message(Msg,Session),
    Session.

at_least_once(Msg,Session) ->
    fwd_message(Msg,Session),
    Session.

%% --------------------------------------------------------------------------------------
%% Storing Packet Identifier and Forwarding the message need to be atomic operations
%% --------------------------------------------------------------------------------------
exactly_once_phase1(Msg = #mqtt_message{packet_id = PacketId, qos = 2},
                    Session = #session_in{packet_seq = Seq, qos2_rec = Qos2Rec})  ->
    case gb_sets:is_element(PacketId,Qos2Rec) of
        true ->

            %% Remember PacketId
            NewRec = gb_sets:add(PacketId,Qos2Rec),

            %% Write-Ahead the message to forward, assigning an unique incremental seq number
            %% so it can be easily de-duplicated at the receiver
            NewSeq = Seq + 1,
            NewSession = Session#session_in{
                packet_seq = NewSeq,
                msg_in_flight = Msg,
                qos2_rec = NewRec},
            maybe_persist(NewSession),

            %% Actually Forward the  message. Because the message has a unique incremental seq number,
            %% this can be performed multiple times during recovery
            fwd_message(Msg,NewSeq),
            maybe_persist(Session#session_in{msg_in_flight = undefined});

        false ->
            Session %% packet already processed, do nothing
    end.

%%  Completes message send
exactly_once_phase2(PacketId,Session = #session_in{qos2_rec = Qos2Rec}) ->
    NewSession = Session#session_in{qos2_rec = gb_sets:delete(PacketId,Qos2Rec)},
    maybe_persist(NewSession).


recover(Session =  #session_in{msg_in_flight = undefined}) ->
    Session;

recover(Session =  #session_in{packet_seq = Seq, msg_in_flight = Msg}) ->
    fwd_message(Msg,Seq),
    maybe_persist(Session#session_in{msg_in_flight = undefined}).

fwd_message(Msg = #mqtt_message{ topic = _Topic},_Seq) ->
    error_logger:info_msg("Processing message ~p~n",[Msg]),
    mqtt_router:global_route(Msg).

maybe_persist(Session = #session_in{is_persistent = _IsPersistent}) ->
    %% @todo: handle persistence
    Session.
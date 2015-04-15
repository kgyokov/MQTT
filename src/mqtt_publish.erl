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
-include("mqtt_internal_msgs.hrl").

%% API
-export([at_most_once/2, at_least_once/2, exactly_once_phase1/2, exactly_once_phase2/2, recover/1, discard_will/1, new/2]).


-record(session_in,{
    client_id                 ::binary(),
    is_persistent  = false    ::boolean(),         %% whether the session needs to be persisted
    packet_seq = 0            ::non_neg_integer(), %% packet sequence number (ever increasing)
    msg_in_flight             ::#mqtt_message{},
    qos2_rec = gb_sets:new()  ::gb_sets:set(),
    will                      ::#will_details{}
}).


%%%===================================================================
%%% API
%%%===================================================================

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
exactly_once_phase1(Msg = #mqtt_message{packet_id = PacketId, qos = ?QOS_2},
                    Session = #session_in{packet_seq = Seq, qos2_rec = Qos2Rec})  ->
    case gb_sets:is_element(PacketId,Qos2Rec) of
        false ->
            NewSession = log_pending_message(Msg,Session),
            send_pending_message(NewSession);
        true ->
            duplicate %% packet already processed, do nothing
    end.

%%  Completes message send
exactly_once_phase2(PacketId,Session = #session_in{qos2_rec = Qos2Rec}) ->
    NewSession = Session#session_in{qos2_rec = gb_sets:del_element(PacketId,Qos2Rec)},
    maybe_persist(NewSession).


recover(Session = #session_in{msg_in_flight = undefined}) ->
    Session;

recover(Session) ->
    send_pending_message(Session).

new(ClientId,Will) ->
    #session_in{client_id = ClientId,will = Will}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

log_pending_message(Msg = #mqtt_message{packet_id = PacketId, qos = ?QOS_2},
                    Session = #session_in{packet_seq = Seq, qos2_rec = Qos2Rec}) ->
    %% Remember PacketId
    NewRec = gb_sets:add(PacketId,Qos2Rec),

    %% Write-Ahead the message to forward, assigning an unique incremental seq number
    %% so it can be easily de-duplicated at the receiver
    NewSeq = Seq + 1,
    MsgToSend = Msg#mqtt_message{seq = NewSeq},
    NewSession = Session#session_in{
        packet_seq = NewSeq,
        msg_in_flight = MsgToSend,
        qos2_rec = NewRec},
    maybe_persist(NewSession).

send_pending_message(Session =  #session_in{packet_seq = Seq, msg_in_flight = Msg}) ->
    %% Actually Forward the  message. Because the message has a unique incremental seq number,
    %% this can be performed multiple times during recovery
    fwd_message(Msg,Seq),
    maybe_persist(Session#session_in{msg_in_flight = undefined}),
    {ok, Session}.

fwd_message(Msg = #mqtt_message{topic = _Topic},_Seq) ->
    error_logger:info_msg("Processing message ~p~n",[Msg]),
    mqtt_router:global_route(Msg).

maybe_persist(Session = #session_in{is_persistent = _IsPersistent}) ->
    %% @todo: handle persistence
    Session.
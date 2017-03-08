%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%% Handles incoming messages and the various
%%% @end
%%% Created : 18. Dec 2014 8:13 PM
%%%-------------------------------------------------------------------
-module(mqtt_publish).
-author("Kalin").

-include("mqtt_pubsub.hrl").
-include("mqtt_internal_msgs.hrl").

%% API
-export([qos0/2, qos1/2, qos2_phase1/2, qos2_phase2/2,
         recover/1, discard_will/1, new/2, maybe_publish_will/1]).


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

qos0(Msg,Session = #session_in{packet_seq = Seq}) ->
    NewSeq = Seq + 1,
    fwd_message(Msg,NewSeq),
    Session#session_in{packet_seq = NewSeq}.

qos1(Msg,Session = #session_in{packet_seq = Seq}) ->
    NewSeq = Seq + 1,
    fwd_message(Msg,NewSeq),
    Session#session_in{packet_seq = NewSeq}.

%% --------------------------------------------------------------------------------------
%% Storing Packet Identifier and Forwarding the message need to be atomic operations
%% --------------------------------------------------------------------------------------
qos2_phase1(Msg = #mqtt_message{packet_id = PacketId, qos = ?QOS_2},
            Session = #session_in{qos2_rec = Qos2Rec})  ->
    case gb_sets:is_element(PacketId,Qos2Rec) of
        false ->
            NewSession = log_pending_message(Msg,Session),
            send_pending_message(NewSession),
            NewSession;
        true ->
            %%duplicate, %% packet already processed, do nothing
            Session
    end.

%%  Completes message send
qos2_phase2(PacketId,Session = #session_in{qos2_rec = Qos2Rec}) ->
    NewSession = Session#session_in{qos2_rec = gb_sets:del_element(PacketId,Qos2Rec)},
    maybe_persist(NewSession).


maybe_publish_will(Session = #session_in{will = undefined}) ->
    Session;

maybe_publish_will(Session = #session_in{will = Will,
                                         client_id = ClientId,
                                         packet_seq = Seq}) ->
    #will_details{content = Content,
                  qos = Qos,
                  retain = Retain,
                  topic = Topic} = Will,

    NewSeq = Seq +1,
    Msg = #mqtt_message{client_id = ClientId,
                        seq = NewSeq,
                        dup = false,
                        content = Content,
                        qos = Qos,
                        retain = Retain,
                        topic = Topic},
    fwd_message(Msg,NewSeq),
    Session.

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
    NewSession = Session#session_in{packet_seq = NewSeq,
                                    msg_in_flight = MsgToSend,
                                    qos2_rec = NewRec},
    maybe_persist(NewSession).

send_pending_message(Session = #session_in{packet_seq = Seq, msg_in_flight = Msg}) ->
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
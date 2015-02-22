%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Feb 2015 5:38 PM
%%%-------------------------------------------------------------------
-author("Kalin").


-record(mqtt_session,{
  client_id,
  packet_id,
  packet_seq,
  qos1,
  qos2,
  qos2_rec,
  refs
}).
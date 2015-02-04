%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Jan 2015 11:54 PM
%%%-------------------------------------------------------------------
-module(mqtt_connection_sup1).
-author("Kalin").

-behaviour(ranch_protocol).

%% API
-export([start_link/4, init/4]).

start_link(Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
  {ok, Pid}.

init(Ref, Socket, Transport, _ProtocolOptions)->
  ok = ranch:accept_ack(Ref),
  loop(Socket,Transport).

loop(Socket, Transport) ->
  case Transport:recv(Socket,0,5000) of
    {ok,Data} ->
      Transport:send(Socket,Data),
      io:write(Data),
      loop(Socket,Transport);
    _ ->
      ok = Transport:close(Socket)
  end.



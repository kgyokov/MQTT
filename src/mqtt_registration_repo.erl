%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Dec 2014 10:28 PM
%%%-------------------------------------------------------------------
-module(mqtt_registration_repo).
-author("Kalin").

-define(REG_TABLE, client_registration).

-record(client_registration, {client_id = undefined, connection_pid, timestamp}).

%% API
-export([register/2, unregister/2, get_registration/1]).

register(Pid, ClientId)->
  NewReg =  #client_registration{
    client_id = ClientId,
    connection_pid = Pid,
    timestamp = time()
  },
  F = fun()->
      %% take write lock
      case mnesia:read(?REG_TABLE, ClientId, write) of
        [] ->
          mnesia:write(?REG_TABLE,NewReg),
          ok;
        [E] when E#client_registration.connection_pid =:= Pid ->
          ok;
        [E] when E#client_registration.connection_pid =/= Pid ->
          mnesia:write(?REG_TABLE,NewReg),
          {duplicate_detected, E}
      end
    end,
    case mnesia:activity(transaction,F,[],mnesia_frag) of
      ok ->
        ok;
      {duplicate_detected, _E} ->
        handle_duplicate(Pid),
        ok
    end
.

unregister(Pid,ClientId)->
  F = fun()->
    %% take write lock
    case mnesia:read(?REG_TABLE, ClientId, write) of
      [] ->
        ok;
      [E] when E#client_registration.connection_pid =:= Pid ->
        mnesia:write(?REG_TABLE,E#client_registration{connection_pid = undefined}),
        ok;
      [E] when E#client_registration.connection_pid =/= Pid ->
        ok
    end
  end,
  mnesia:activity(transaction,F,[],mnesia_frag).

get_registration(ClientId)->
  mnesia:dirty_read(?REG_TABLE, ClientId).


handle_duplicate(Pid)->
  mqtt_connection:close_duplicate(Pid).

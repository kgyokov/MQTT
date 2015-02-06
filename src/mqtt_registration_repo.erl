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

-record(client_reg, {client_id, connection_pid, session_id,  timestamp}).

%% API
-export([register/3, unregister/2, get_registration/1, create_tables/0]).

%%
%% @doc
%%
%% Creates the mnesia tables
%%
%% @end

create_tables()->
  mnesia:create_schema("")
.



%%
%% @doc
%%
%% Registers a Process as responsible for the connection to a given ClientId. Only one Process can be registered
%% per client
%%
%% @end

register(Pid, ClientId, SessionId)->
  NewReg = #client_reg{client_id = ClientId,connection_pid = Pid,
                       session_id = SessionId,timestamp = time()},
  F = fun()->
      %% take write lock
      case mnesia:read(?REG_TABLE, ClientId, write) of
        [] ->
          mnesia:write({?REG_TABLE,NewReg}),
          ok;
        [E = #client_reg{connection_pid = EPid}] ->
          case EPid of
            Pid ->
              ok;
            undefined ->
              mnesia:write({?REG_TABLE,NewReg}),
              ok;
            _ ->
              mnesia:write({?REG_TABLE,NewReg}),
              {duplicate_detected, E}
          end
      end
    end,
    case mnesia:activity(transaction,F,[],mnesia_frag) of
      ok ->
        ok;
      {duplicate_detected, #client_reg{connection_pid = ExistingPid}} ->
        handle_duplicate(ExistingPid),
        ok
    end
.



%%
%% @doc
%%
%% Unregisters a Process responsible for the connection to a given <b>ClientId</b>. Sets the <b>Pid</b> to <i>undefined</i>
%%
%% @end
unregister(Pid,ClientId)->
  F = fun()->
    %% take write lock
    case mnesia:read(?REG_TABLE, ClientId, write) of
      [] ->
        ok;
      [E = #client_reg{connection_pid = EPid}] when EPid =:= Pid ->
        mnesia:write({?REG_TABLE,E#client_reg{connection_pid = undefined}}),
        ok;
      [#client_reg{connection_pid = EPid}] when EPid =/= Pid ->
        ok
    end
  end,
  mnesia:activity(transaction,F,[],mnesia_frag).

get_registration(ClientId)->
  mnesia:dirty_read(?REG_TABLE, ClientId).


handle_duplicate(Pid)->
  mqtt_connection:close_duplicate(Pid).

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Feb 2015 11:33 PM
%%%-------------------------------------------------------------------
-module(mqtt_claims_auth).
-author("Kalin").

-behaviour(gen_auth).

-record(mqtt_auth_state, {
})

%% API
-export([authenticate/3, authorize/3]).

authenticate(ClientId, Username, Password) ->
  erlang:error(not_implemented).


authorize(AuthS, Action, Resource) ->
  .

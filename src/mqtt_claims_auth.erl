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

%% -record(mqtt_auth_state, {
%% }).

%% API
-export([authenticate/3, authorize/3]).

authenticate(ClientId, Username, Password) ->
  erlang:error(not_implemented).

authorize(AuthS, Action, Resource) ->
%%   ClaimType = case Action of
%%       publish ->
%%         publish_to;
%%       subscribe ->
%%         subscribe_to
%%   end,
  case is_covered_by_claim(AuthS, Action, Resource) of
    true ->
      ok;
  false ->
    {error,unauthroized}
  end.


is_covered_by_claim(AuthS,ClaimType,Topic)->
  case dict:find(ClaimType,AuthS) of
    {ok,Claims} ->
      lists:any(fun(ClaimVal)-> mqtt_topic:is_covered_by(Topic,ClaimVal) end,Claims);
    error ->
      false
  end.


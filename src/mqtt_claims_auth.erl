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
-export([authenticate/4, authorize/3]).

authenticate(Configuration, ClientId, Username, Password) ->
  try
  [
    case ClaimsGenerator:get_claims(Options,ClientId,Username,Password) of
      {ok,Claims}->
        Claims;
      not_applicable ->
        [];
      {error,Reason} ->
        throw({error,Reason})
    end
  || {ClaimsGenerator,Options} <- Configuration
  ] of ClaimsList -> AllClaims = lists:concat(ClaimsList),
                     lists:foldr(
                          fun({ClaimType,ClaimVal},Dict)-> dict:append_list(ClaimType,ClaimVal,Dict) end,
                          dict:new(), AllClaims)
  catch
  throw:{error,Reason} ->
    {error,Reason}
end
.

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


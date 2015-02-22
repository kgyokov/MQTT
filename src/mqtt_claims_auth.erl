%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Claims-based authentication/authorization
%%% @end
%%% Created : 04. Feb 2015 11:33 PM
%%%-------------------------------------------------------------------
-module(mqtt_claims_auth).
-author("Kalin").

-behaviour(gen_auth).

-type claims_dict() :: any().

%%%===================================================================
%%% API
%%%===================================================================
-export([authenticate/4, authorize/3]).

-spec authenticate([{ClaimsGenerator::module(),Options::any()}],
    binary(), binary(), binary()) ->
  claims_dict() | {error, any()}.
authenticate(Configuration, ClientId, Username, Password) ->
  try
  [
    case ClaimsGenerator:get_claims(Options,ClientId,Username,Password) of
      {ok,Claims} ->
        Claims;
      not_applicable  ->
        [];
      {error,Reason} ->
        throw({auth_error,Reason})
    end
  || {ClaimsGenerator,Options} <- Configuration
  ] of ClaimsLists ->  AllClaims = lists:concat(ClaimsLists),
                       claims_to_dictionary(AllClaims)
  catch
  throw:{auth_error,Reason} ->
    {error,Reason}
end.

-spec authorize(claims_dict(), any(), any()) -> ok | {error,any()}.
authorize(AuthCtx, Action, Resource) ->
  case is_covered_by_claim(AuthCtx, Action, Resource) of
    true ->
      ok;
    false ->
      {error,unauthroized}
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

claims_to_dictionary(AllClaims)->
  lists:foldr(
    fun({ClaimType,ClaimVal},Dict)-> dict:append_list(ClaimType,ClaimVal,Dict) end,
    dict:new(), AllClaims).

is_covered_by_claim(AuthCtx,ClaimType,Topic)->
  case dict:find(ClaimType,AuthCtx) of
    {ok,Claims} ->
      lists:any(fun(ClaimVal)-> mqtt_topic:is_covered_by(Topic,ClaimVal) end,Claims);
    error ->
      false
  end.

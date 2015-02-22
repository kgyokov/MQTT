%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Authorizes everything
%%% @end
%%% Created : 21. Feb 2015 9:02 PM
%%%-------------------------------------------------------------------
-module(gen_auth_default).
-author("Kalin").
-behaviour(gen_auth).

%% API
-export([authenticate/4, authorize/3]).

authenticate(_Configuration, _ClientId, _Username, _Password) ->
  {ok, default}.

authorize(default, _Action, _Resource) ->
  ok.

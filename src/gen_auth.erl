%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Feb 2015 11:11 PM
%%%-------------------------------------------------------------------
-module(gen_auth).
-author("Kalin").

-callback authenticate(
    Options::any(),
    ClientId::binary(),
    Username::binary(),
    Password::binary()
)->
  {ok, AuthS::any()} |        %% AuthS can contain things like claims, etc.
  {error,Reason::any()}.     %% error, e.g. invalid password

-callback authorize(
    AuthS::term(),
    Action::atom(),
    Resource::any()
)-> ok | {error,Details::any()}.

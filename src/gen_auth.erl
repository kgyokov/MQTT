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
    Configuration::any(),
    ClientId::binary(),
    Username::binary(),
    Password::binary()
)->
    {ok, AuthCtx::any()} |     %% AuthCtx can contain things like claims, etc.
    {error,Reason::bad_credentials | any()}.     %% error, e.g. invalid password

-callback authorize(
    AuthCtx::any(),
    Action::atom(),
    Resource::any()
)-> ok | {error,Reason::any()}.

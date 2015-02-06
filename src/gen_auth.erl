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
    ClientId::string(),
    Username::string(),
    Password::string()
)-> {ok, AuthS::any()} | {error,Reason::any()}.

-callback authorize(
    AuthS::term(),
    {Action::atom(),Resource::any()}
)-> ok | {error,Details::any()}.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Feb 2015 11:38 PM
%%%-------------------------------------------------------------------
-module(claims_x509).
-author("Kalin").

%% API
-export([]).

get_claims(Options,ClientId,Username,Password)->
	IssuerID = socket_to_claims(undefined).

socket_to_claims(Socket) ->
	case ssl:peercert(Socket) of
		{error, no_peercert} ->
			[];
		{ok, Cert} ->
			{ok, IssuerID} = public_key:pkix_issuer_id(Cert, self),
			IssuerID
	end,
	ok.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%% Quickly slapped-together lazy lists. Should really switch to Elixir some day.
%%% @end
%%% Created : 07. Feb 2015 8:47 PM
%%%-------------------------------------------------------------------
-module(iter).
-author("Kalin").

-define(LAZY(T),fun() -> T end).

%% API
-export([head/1, tail/1, take/2, concat/1, foldl/3, to_iter/3, seq/1, to_list/1, map/2, from_list/1]).

head({H,_})  -> H.
tail({_,T}) -> T().

seq(Start) -> {Start,?LAZY(seq(Start+1))}.

take(0,_)     -> nil;
take(_,nil)   -> nil;
take(N,{H,T}) -> {H,?LAZY(take(N-1,T()))}.


concat(nil)     -> nil;
concat({H,T})   -> concat1(H,T()).

concat1(nil,  nil)   -> nil;
concat1(nil,  Iter2) -> concat(Iter2);
concat1({H,T},Iter2) -> {H,?LAZY(concat1(T(),Iter2))}.


foldl(nil,_,Acc)   -> Acc;
foldl({H,T},Fun,Acc) ->
    Acc1 = Fun(H,Acc),
    foldl(T(),Fun,Acc1).

map(_,  nil)   -> nil;
map(Fun,{H,T}) -> {Fun(H),?LAZY(map(Fun,T()))}.

%%to_iter(none,Fun,_Mod) -> nil;
%%to_iter(BasicIter,Fun,_Mod) ->
%%    {H,T} = Fun(BasicIter),
%%    {H,?LAZY(to_iter(Mod:next(T),Mod))}.

to_iter(none,_,_)        -> nil;
to_iter({H,T},Mod,Fun)   -> {H,?LAZY(to_iter(Mod:Fun(T),Mod,Fun))}.

to_list(L) -> lists:reverse(to_list(L,[])).
to_list(nil,Acc) -> Acc;
to_list({H,T},Acc) -> to_list(T(),[H|Acc]).

from_list([])     -> nil;
from_list([H|T])  -> {H,?LAZY(from_list(T))}.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Based on Finger Trees (http://www.staff.city.ac.uk/~ross/papers/FingerTree.html)
%%% A mostly direct translation to Erlang
%%% @end
%%%-------------------------------------------------------------------
-module(finger_tree).
-author("Kalin").

%% API
-export([pushl/2, pushr/2, is_empty/1, headl/1, taill/1]).

%%
%% Monoid behavior
%%
-callback identity(T) -> T.
-callback binary_op(T,T) -> T.

-define(SUSP(Expr), fun() -> Expr end).
-define(FORCE(Expr),Expr()).


pushl(A,empty)                  ->  {single,A};
pushl(A,{single,B})             ->  {deep,[A],empty,[B]};
pushl(A,{deep,[B,C,D,E],M,SF})  ->  {deep,[A,B],pushl({node3,C,D,E},M),SF};
pushl(A,{deep,PR,M,SF})         ->  {deep,[A|PR],M,SF}.

pushr(empty,A)                  ->  {single,A};
pushr({single,B},A)             ->  {deep,[B],empty,[A]};
pushr({deep,PR,M,[E,D,C,B]},A)  ->  {deep,PR,pushr({node3,E,D,C},M),[B,A]};
pushr({deep,PR,M,SF},A)         ->  {deep,PR,M,SF++[A]}.


%%
%% Reducers
%%

reducer(Fun,{node2,A,B},Z)      -> Fun(A,Fun(B,Z));
reducer(Fun,{node3,A,B,C},Z)    -> Fun(A,Fun(B,Fun(C,Z)));

reducer(_Fun,empty,Z)           -> Z;
reducer(Fun,{single,X},Z)       -> Fun(X,Z);
reducer(Fun,{deep,PR,M,SF},Z)   ->
%%    R1 = reducer(Fun,SF,Z),
%%    R2 = reducer(reducer(Fun,))
    Fun1 = fun(A,B) -> reducer(Fun,A,B)  end,
    Fun2 = fun(A,B) -> reducer(Fun2,A,B) end,
    Fun1(PR,Fun2(M,Fun1(SF,Z)));

reducer(Fun,L,Z) when is_list(L) -> lists:foldr(Fun,Z,L).


reducel(Fun,Z,{node2,B,A})      -> Fun(Fun(Z,B),A);
reducel(Fun,Z,{node3,C,B,A})    -> Fun(Fun(Fun(Z,C),B),A);

reducel(_Fun,Z,empty)           -> Z;
reducel(Fun,Z,{single,X})       -> Fun(X,Z);
reducel(Fun,Z,{deep,PR,M,SF})   ->
    Fun1 = fun(A,B) -> reducel(Fun,A,B)  end,
    Fun2 = fun(A,B) -> reducel(Fun2,A,B) end,
    Fun1(Fun2(Fun1(Z,PR),M),SF);

reducel(Fun,Z,L) when is_list(L) ->
    RFun = fun(Acc,Elem) -> Fun(Elem,Acc) end,
    lists:foldl(RFun,Z,L).

%%
%% Conversion
%%

to_tree(L) -> lists:foldl(fun pushr/2,empty,L).
to_list(T) -> reducer(fun(H,T) -> [H|T] end,T,[]).


%%
%% Navigation
%%

viewl(empty)                -> nil;
viewl({single,A})           -> {A,nil};
viewl({deep,[H|T],M,SF})    -> {H,deepl(T,M,SF)}.

deepl([],M,SF)  ->
    case viewl(M) of
        nil     -> to_tree(SF);
        {A,M1}  -> {deep,to_list(A),M1,SF}
    end;

deepl(PR,M,SF)  ->
    {deep,PR,M,SF}.

headl(T) ->
    {H,_} = viewl(T),
    H.

taill(T) ->
    {_,T} = viewl(T),
    T.

is_empty(T) ->
    case viewl(T) of
        nil     -> true;
        {_,_}   -> false
    end.



%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Based on Finger Trees (http://www.staff.city.ac.uk/~ross/papers/FingerTree.html)
%%% Mostly direct translation to Erlang from the Haskell code in the paper
%%% @end
%%%-------------------------------------------------------------------
%% -module(finger_tree).
-author("Kalin").

%%-include("finger_tree.hrl").

%%-define(MOD,finger_tree_monoid@).
-define(MOD,?MODULE).


%% API
-export([pushl/2, pushr/2, is_empty/1, headl/1, taill/1, concat/2, measure/1, empty/0,
    to_list/1, headr/1, tailr/1, split_tree/3, split/2]).

-type tree_node(V,E) ::
{node2,V,E,E}
|{node3,V,E,E}.

-type finger_tree(V,E) ::
    empty
    |{single,E}
    |{deep,V,[E],finger_tree(V,tree_node(V,E)),[E]}.

empty() -> empty.

-spec pushl(E,finger_tree(V,E)) -> finger_tree(V,E).
-spec pushr(finger_tree(V,E),E) -> finger_tree(V,E).

pushl(A,empty)                      ->  {single,A};
pushl(A,{single,B})                 ->  deep([A],empty,[B]);
pushl(A,{deep,_,[B,C,D,E],M,SF})    ->  deep([A,B],pushl(node3(C,D,E),M),SF);
pushl(A,{deep,_,PR,M,SF})           ->  deep([A|PR],M,SF).

pushr(empty,A)                      ->  {single,A};
pushr({single,B},A)                 ->  deep([B],empty,[A]);
pushr({deep,_,PR,M,[E,D,C,B]},A)    ->  deep(PR,pushr(M,node3(E,D,C)),[B,A]);
pushr({deep,_,PR,M,SF},A)           ->  deep(PR,M,SF++[A]).


%%
%% Reducers
%%

-spec reducer(Fun,XS,Z) -> Z when
    Fun :: fun((E,Z) -> Z),
    XS ::
        tree_node(_,E)
        |finger_tree(_,E)
        |[E].
-spec reducel(Fun,Z,XS) -> Z when
    Fun :: fun((Z,E) -> Z),
    XS ::
        tree_node(_,E)
        |finger_tree(_,E)
        |[E].

reducer(Fun,{node2,_,A,B},Z)    -> Fun(A,Fun(B,Z));
reducer(Fun,{node3,_,A,B,C},Z)  -> Fun(A,Fun(B,Fun(C,Z)));
reducer(_Fun,empty,Z)           -> Z;
reducer(Fun,{single,X},Z)       -> Fun(X,Z);
reducer(Fun,{deep,_,PR,M,SF},Z) ->
    Fun1 = fun(A,B) -> reducer(Fun,A,B)  end,
    Fun2 = fun(A,B) -> reducer(Fun1,A,B) end,
    Fun1(PR,Fun2(M,Fun1(SF,Z)));
%% NOTE: During recursive reducer calls, it is important that we restrict the level of nesting to 2,
%% as shown above. We do not want to perform blind recursion, because
%% reducer(fun(node(E),Z),finger_tree(E)) is NOT the same as reducer(fun(node(E),Z),finger_tree(node(E)))
reducer(Fun,L,Z) when is_list(L) -> lists:foldr(Fun,Z,L).


reducel(Fun,Z,{node2,_,B,A})    -> Fun(Fun(Z,B),A);
reducel(Fun,Z,{node3,_,C,B,A})  -> Fun(Fun(Fun(Z,C),B),A);
reducel(_Fun,Z,empty)           -> Z;
reducel(Fun,Z,{single,X})       -> Fun(Z,X);
reducel(Fun,Z,{deep,_,PR,M,SF}) ->
    Fun1 = fun(A,B) -> reducel(Fun,A,B)  end,
    Fun2 = fun(A,B) -> reducel(Fun1,A,B) end,
    Fun1(Fun2(Fun1(Z,PR),M),SF);

reducel(Fun,Z,L) when is_list(L) ->
    RFun = fun(Acc,Elem) -> Fun(Elem,Acc) end,
    lists:foldl(RFun,Z,L).

%%
%% Conversion
%%

-spec to_tree([E]) -> finger_tree(_,E).
-spec to_list(finger_tree(_,E)) -> [E].
to_tree(L) when is_list(L) -> lists:foldr(fun pushl/2,empty,L).
to_list(T) -> reducer(fun(H,XS) -> [H|XS] end,T,[]).


%%
%% Navigation
%%

%% @todo: lazy view??? -deepl and viewl call each other recursively

%%
%% Left implementation
%%

viewl(empty)                -> nil;
viewl({single,A})           -> {A,fun() -> empty end};
viewl({deep,_,[H|T],M,SF})  -> {H,fun() -> deepl(T,M,SF) end}.

deepl([],M,SF)  ->
    case viewl(M) of
        nil     -> to_tree(SF);
        {A,M1}  -> deep(to_list(A),M1(),SF)
    end;

deepl(PR,M,SF)  ->
    deep(PR,M,SF).

-spec headl(finger_tree(_,E)) -> E.
headl(T) ->
    {H,_} = viewl(T),
    H.

-spec taill(finger_tree(V,E)) -> finger_tree(V,E).
taill(T) ->
    {_,T1} = viewl(T),
    T1().


%%
%% Right implementation
%%

viewr(empty)                -> nil;
viewr({single,A})           -> {A,fun() -> empty end};
viewr({deep,_,PR,M,SF})     ->
    %% todo: Don't use lists for PR and SF, or use normal cons to push elements to SF
    [H|T] = lists:reverse(SF),
    T1 = lists:reverse(T),
    {H,fun() -> deepr(PR,M,T1) end}.

deepr(PR,M,[])  ->
    case viewr(M) of
        nil     -> to_tree(PR);
        {A,M1}  -> deep(PR,M1(),to_list(A))
    end;

deepr(PR,M,SF)  ->
    deep(PR,M,SF).

-spec headr(finger_tree(_,E)) -> E.
headr(T) ->
    {H,_} = viewr(T),
    H.

-spec tailr(finger_tree(V,E)) -> finger_tree(V,E).
tailr(T) ->
    {_,T1} = viewr(T),
    T1().

-spec is_empty(finger_tree(_,any())) -> true|false.
is_empty(T) ->
    case viewl(T) of
        nil     -> true;
        {_,_}   -> false
    end.


%%
%% Appending
%%

%% @todo: reducer and pushl???

-spec app3(finger_tree(V,E),[E],finger_tree(V,E)) -> finger_tree(V,E).

app3(empty,TS,XS)           -> reducer(fun pushl/2,TS,XS); %% @todo: optimize???
app3(XS,TS,empty)           -> reducel(fun pushr/2,XS,TS);
app3({single,X},TS,XS)      -> pushl(X,reducer(fun pushl/2,TS,XS));
app3(XS,TS,{single,X})      -> pushr(reducel(fun pushr/2,XS,TS),X);

app3({deep,_,PR1,M1,SF1},TS,{deep,_,PR2,M2,SF2}) ->
    M3 = app3(M1,to_nodes(SF1++TS++PR2),M2),
    deep(PR1,M3,SF2).

-spec to_nodes([E]) -> [tree_node(_,E)].
to_nodes([A,B])        -> [node2(A,B)];
to_nodes([A,B,C])      -> [node3(A,B,C)];
to_nodes([A,B,C,D])    -> [node2(A,B),node2(C,D)];
to_nodes([A,B,C|T])    -> [node3(A,B,C)|to_nodes(T)]. %% @todo: optimize???


concat(XS,TS) -> app3(XS,[],TS).

%%
%% Measuring
%%

%%
%% This might work much better with Elixir protocols, because we can define the Monoid {Id,As} using a protocol
%% instead of passing it around like crazy. Parameterized modules also would have worked :-(

node2(A,B) ->
    V = measure(A),
    V1 = measure(B),
    {node2,?MOD:as(V,V1),A,B}.

node3(A,B,C)  ->
    V = measure(A),
    V1 = measure(B),
    V2 = measure(C),
    V3 = ?MOD:as(?MOD:as(V,V1),V2),
    {node3,V3,A,B,C}.

deep(PR,M,SF) ->
    V = measure(PR),
    V1 = measure(M),
    V2 = measure(SF),
    V3 = ?MOD:as(?MOD:as(V,V1),V2),
    {deep,V3,PR,M,SF}.

%% Measure Nodes
measure({node2,V,_,_})          -> V;
measure({node3,V,_,_,_})        -> V;
measure(D) when is_list(D)      -> reducel(fun(Z,E) -> ?MOD:as(Z,measure(E)) end,?MOD:id(),D);
%% Measure Tree
measure(empty)                  -> ?MOD:id();
measure({single,X})             -> measure(X);
measure({deep,V,_,_,_})         -> V;
measure(X)                      -> ?MOD:ms(X).



%%
%% Splitting
%%
-type split(F,A)::{split,F,A,F}.

-spec split(Pred,T) -> {T,T} when
    Pred::fun((V) -> boolean()),
    T::finger_tree(V,_).

split(_,empty)   -> {empty,empty};
split(Pred,T)       ->
    {split,L,X,R} = split_tree(Pred,?MOD:id(),T),
    case Pred(measure(T)) of
        true    -> {L,pushl(X,R)};
        false   -> {T,empty}
    end.

-spec split_digit(Pred,V,[A]) -> {split,[A],A,[A]} when
    Pred::fun((V) -> boolean()).

split_digit(_,_,[A]) -> {split,[],A,[]};

split_digit(Pred,V,[A|AS]) ->
    V1 = ?MOD:as(V,measure(A)),
    case Pred(V1) of
        true    -> {split,[],A,AS};
        false   ->
            {split,L,X,R} = split_digit(Pred,V1,AS),
            {split,[A|L],X,R}
    end.

-spec split_tree(Pred,V,T) -> split(T,A) when
    Pred::fun((V) -> boolean()),
    T::finger_tree(V,A).

split_tree(_,_,{single,A}) -> {split,empty,A,empty};

split_tree(Pred,V,{deep,_,PR,M,SF}) ->
    VPR = ?MOD:as(V,measure(PR)),
    case Pred(VPR) of
        true ->
            {split,L,X,R} = split_digit(Pred,V,PR),
            {split,to_tree(L),X,deepl(R,M,SF)};
        false ->
            VM = ?MOD:as(VPR,measure(M)),
            case Pred(VM) of
                true ->
                    {split,ML,XS,MR} = split_tree(Pred,VPR,M),
                    {split,L,X,R} = split_digit(Pred,?MOD:as(VPR,measure(ML)),to_list(XS)),
                    {split,deepr(PR,ML,L),X,deepl(R,MR,SF)};
                false ->
                    {split,L,X,R} = split_digit(Pred,VM,SF),
                    {split,deepr(PR,M,L),X,to_tree(R)}
            end
    end.
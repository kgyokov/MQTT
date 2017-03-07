%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Maintains a set of {Key,Val} pairs where min(Val) can be updated/retrieved efficiently.
%%% NOTE: The Val for a key can be replaced only by a larger Val. See 'store' function.
%%% @todo: Maybe factor that requirement out.
%%% @todo: This is naive. Come up w/ a better data structure
%%% @end
%%% Created : 12. Jan 2016 12:27 AM
%%%-------------------------------------------------------------------
-module(min_val_tree).
-author("Kalin").

%% API
-export([store/3, min/1, split/2, remove/2, new/0, new/1, get_val/2, is_empty/1, iterator/1]).

-opaque tree(Key,Val) :: {gb_trees:tree(Val,Key), dict:dict(Val,Key), fun((Val,Val) -> boolean())}.
-opaque tree() :: tree(_,_).

-export_type([tree/2,tree/0]).

new() -> new(fun(A,B) -> A =< B end).

-spec(new(fun((Val,Val) -> boolean())) -> tree(any(),Val)).
new(Compare) ->
    {
        gb_trees:empty(),   %% A tree of Vals. Each Val can have several keys associated with it
        dict:new(),         %% A tree of Keys, Every key has one corresponding Val associated with it,
        Compare             %% A function to compare the Vals (true if A =< B)
    }.

iterator({ByVal,_,_}) ->
    GbIter = gb_trees:iterator(ByVal),
    Iter = gb_tree_to_iter(GbIter),
    Iter1 = iter:map(fun gb_set_to_iter/1,Iter),
    iter:concat(Iter1).

gb_set_to_iter({Val,Keys}) ->
    GbIter = gb_sets:iterator(Keys),
    Iter = iter:to_iter(GbIter,gb_sets),
    iter:map(fun(Key) -> {Key,Val} end,Iter).

gb_tree_to_iter(GbIter) -> gb_tree_to_iter1(gb_trees:next(GbIter)).

gb_tree_to_iter1(none)      -> nil;
gb_tree_to_iter1({K,V,T})   -> {{K,V},fun() -> gb_tree_to_iter1(gb_trees:next(T)) end}.

%%next(Iter = {_,_,_}) -> next_key(Iter);
%%next(ValIter)        -> next_val(ValIter).
%%
%%next_key({Val,KeyIter,ValIter}) ->
%%    case gb_sets:next(KeyIter) of
%%        {Key,KeyIter1} -> {{Key,Val},{Val,KeyIter1,ValIter}};
%%        none -> next_val(ValIter)
%%    end.
%%
%%next_val(ValIter) ->
%%    case gb_trees:next(ValIter) of
%%        none -> nil;
%%        {Val1,Keys,ValIter1} ->
%%            next_key({Val1,gb_sets:iterator(Keys),ValIter1})
%%    end.
%%
%%
%%
%%append1(none,T2)    -> append2(next(T2));
%%append1({H1,T1},T2) -> {H1,append1(next(T1),T2)}.
%%append2({H,T2})     -> append1(next(H),T2).

%% @doc
%% Takes the Num smallest elements from the data structure and returns them plus the remainder
%% @end
-spec split(non_neg_integer(),tree(Key,Val)) -> {[{Key,Val}],tree(Key,Val)}.

split(Num,Tree) ->
    case is_empty(Tree) of
        true -> {[],Tree};
        false ->
            {Acc,Tree1} = split(Num,Tree,[]),
            {lists:reverse(Acc),Tree1}
    end.


split(Num,T,Acc) when Num =<0 -> {Acc,T};
split(Num,T,Acc) ->
    case view(T) of
        nil -> {Acc,T};
        {H,T1} -> split(Num-1,T1,[H|Acc])
    end.

-spec view(tree(Key,Val)) -> nil | {{Key,Val},tree(Key,Val)}.

view({ByVal,ByKey,Comp}) ->
    case gb_trees:is_empty(ByVal) of
        true ->
            nil;
        false ->
            %% @todo: better way to remove the head
            %% find the key corresponding to the head
            {Val,Keys} = gb_trees:smallest(ByVal),
            [HKey|_] = gb_sets:to_list(Keys),
            {{HKey,Val},
                %% remove the key corresponding to the head
                {delete_key_from_val_idx(HKey,Val,ByVal),
                 dict:erase(HKey,ByKey),
                 Comp}}
    end.

get_val(Key,{_,ByKey,_}) ->
    case dict:find(Key,ByKey) of
        error -> none;
        {ok,Val} -> {ok,Val}
    end.

-spec remove(Key,tree(Key,Val)) -> tree(Key,Val).

remove(Key,T = {ByVal,ByKey,Comp}) ->
    case dict:find(Key,ByKey) of
        error ->
            T;
        {ok,Val} ->
            {delete_key_from_val_idx(Key,Val,ByVal),
             dict:erase(Key,ByKey),
             Comp}
    end.

-spec store(Key,Val,tree(Key,Val)) -> tree(Key,Val).

store(Key,Val,T = {ByVal,ByKey,Comp}) ->
    case dict:find(Key,ByKey) of
        error ->
            insert(Key,Val,T);
        {ok,OldVal}  ->
            case Comp(OldVal,Val) of
                true ->
                    ByVal1 = delete_key_from_val_idx(Key,OldVal,ByVal),
                    ByVal2 = upsert_val_idx(Key,Val,ByVal1),
                    ByKey1 = dict:store(Key,Val,ByKey),
                    {ByVal2,ByKey1,Comp};
                _ ->
                    T %% maintain monotonicity of Val - it can only increase (should we really do this here?)
            end
    end.

-spec is_empty(tree(_,_)) -> boolean().

is_empty({ByVal,_ByKey,_Comp}) ->
    gb_trees:is_empty(ByVal).

%% @doc
%% Gets the smallest value in the tree (may correspond to multiple keys, therefore we only return the Val)
%% @end
-spec min(tree(_,Val)) -> none | {ok,Val}.

min({ByVal,_ByKey,_Comp}) ->
    case gb_trees:is_empty(ByVal) of
        true ->
            none;
        false ->
            {Val,_} = gb_trees:smallest(ByVal),
            {ok,Val}
    end.


%% ===========================================================================================
%% PRIVATE
%% ===========================================================================================

-spec insert(Key,Val,tree(Key,Val)) -> tree(Key,Val).

insert(Key,Val,{ByVal,ByKey,Comp}) ->
    ByVal1 = upsert_val_idx(Key,Val,ByVal),
    ByKey1 = dict:store(Key,Val,ByKey),
    {ByVal1,ByKey1,Comp}.

upsert_val_idx(Key,Val,ByVal) ->
    case gb_trees:is_defined(Val,ByVal) of
        true ->
            add_key_to_val_idx(Key,Val,ByVal);
        false ->
            Keys = gb_sets:insert(Key,gb_sets:new()),
            gb_trees:insert(Val,Keys,ByVal)
    end.

add_key_to_val_idx(Key,Val,ByVal) ->
    Keys = gb_trees:get(Val,ByVal),
    Keys1 = gb_sets:add(Key,Keys),
    gb_trees:update(Val,Keys1, ByVal).

delete_key_from_val_idx(Key,Val,ByVal) ->
    Keys = gb_trees:get(Val,ByVal),
    Keys1 = gb_sets:delete(Key,Keys),
    case gb_sets:size(Keys1) of
        0 -> gb_trees:delete(Val,ByVal);
        _ -> gb_trees:update(Val,Keys1,ByVal)
    end.

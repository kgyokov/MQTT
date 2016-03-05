%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Maintains a set of {Key,Val} pairs where min(Val) can be updated/retrieved efficiently
%%% @todo: This is naive. Come up w/ a better data structure
%%% @end
%%% Created : 12. Jan 2016 12:27 AM
%%%-------------------------------------------------------------------
-module(min_val_tree).
-author("Kalin").

%% API
-export([store/3, min/1, split/2, remove/2, new/0, new/1, get_val/2, is_empty/1]).

new() -> new(fun(A,B) -> A =< B end).

new(Compare) ->
    {
        gb_trees:empty(),   %% A tree of Vals. Each Val can have several keys associated with it
        dict:new(),         %% A tree of Keys, Every key has one corresponding Val associated with it,
        Compare             %% A function to compare the Vals (true if A =< B)
    }.

%% @doc
%% Takes the Num smallest elements from the data structure and returns them plus the remainder
%% @end
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

remove(Key,T = {ByVal,ByKey,Comp}) ->
    case dict:find(Key,ByKey) of
        error ->
            T;
        {ok,Val} ->
            {delete_key_from_val_idx(Key,Val,ByVal),
             dict:erase(Key,ByKey),
             Comp}
    end.

store(Key,Val,T = {ByVal,ByKey,Comp}) ->
    case dict:find(Key,ByKey) of
        error ->
            insert(Key,Val,T);
        {ok,OldVal}  ->
            case Comp(OldVal,Val) of
                true ->
                    ByKey1 = dict:store(Key,Val,ByKey),
                    ByVal1 = delete_key_from_val_idx(Key,OldVal,ByVal),
                    ByVal2 = add_key_to_val_idx(Key,Val,ByVal1),
                    {ByKey1,ByVal2,Comp};
                _ ->
                    T %% maintain monotonicity of Val - it can only increase
            end
    end.

is_empty({ByVal,_ByKey,_Comp}) ->
    gb_trees:is_empty(ByVal).

%% @doc
%% Gets the smallest value in the tree (may correspond to multiple keys, therefore we only return the Val)
%% @end
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

insert(Key,Val,{ByVal,ByKey,Comp}) ->
    ByKey1 = dict:store(Key,Val,ByKey),
    ByVal1 =
        case gb_trees:is_defined(Val, ByVal) of
            true ->
                add_key_to_val_idx(Key,Val,ByVal);
            false ->
                Keys = gb_sets:insert(Key,gb_sets:new()),
                gb_trees:insert(Val,Keys, ByVal)
        end,
    {ByVal1,ByKey1,Comp}.


delete_key_from_val_idx(Key,Val,ByVal) ->
    Keys = gb_trees:get(Val,ByVal),
    Keys1 = gb_sets:delete(Key,Keys),
    case gb_sets:size(Keys1) of
        0 -> gb_trees:delete(Val,ByVal);
        _ -> gb_trees:update(Val,Keys1,ByVal)
    end.

add_key_to_val_idx(Key,Val,ByVal) ->
    Keys = gb_trees:get(Val, ByVal),
    Keys1 = gb_sets:add(Key,Keys),
    gb_trees:update(Val,Keys1, ByVal).

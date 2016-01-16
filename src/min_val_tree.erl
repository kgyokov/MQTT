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
-export([store/3, get_min/1, remove/2, new/0]).


new() ->
    {
        gb_trees:empty(),   %% A tree of Vals. Each Val can have several keys associated with it
        dict:new()    %% A tree of Keys, Every key has one corresponding Val associated with it
    }.

remove(Key,{ByVal,ByKey}) ->
    case dict:find(Key,ByKey) of
        error ->
            {ByVal,ByKey};
        {ok,Val} ->
            {delete_key_from_val_idx(Key,Val,ByVal),
             dict:erase(Key,ByKey)}
    end.

store(Key,Val,{ByVal,ByKey}) ->
    case dict:find(Key,ByKey) of
        error ->
            insert(Key,Val,{ByVal,ByKey});
        {ok,Val} ->
            {ByVal,ByKey};
        {ok,OldVal} ->
            ByKey1 = dict:store(Key,Val,ByKey),
            ByVal1 = delete_key_from_val_idx(Key,OldVal,ByVal),
            ByVal2 = add_key_to_val_idx(Key,Val,ByVal1),
            {ByKey1,ByVal2}
    end.

get_min({ByVal,_ByKey}) ->
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

insert(Key,Val,{ByVal,ByKey}) ->
    ByKey1 = dict:store(Key,Val,ByKey),
    ByVal1 =
        case gb_trees:is_defined(Val, ByVal) of
            true ->
                add_key_to_val_idx(Key,Val,ByVal);
            false ->
                Keys = gb_sets:insert(Key,gb_sets:new()),
                gb_trees:insert(Val,Keys, ByVal)
        end,
    {ByVal1,ByKey1}.


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

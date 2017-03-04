%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Maintains different versions of a dictionary based on sequential Version number.
%%% A memory optimization so we can keep different lists of retained messages per client
%%% Implemented in a NAIVE way right now - by using gb_trees with negative keys.
%%% @todo: more generic implementation
%%% @end
%%% Created : 30. Mar 2016 10:32 PM
%%%-------------------------------------------------------------------
-module(versioned_set).
-author("Kalin").

%% API
-export([new/0, new/2, append/4, get_at/2, iterator_from/3, truncate/2, size/1, remove/3, take/2, next/1, to_list/1, update/3]).

-export_type([set/1]).

-record(s_set,{
    log :: gb_trees:tree(neg_integer()|0,_)
}).

-opaque set(Struct)::#s_set{log:: gb_trees:tree(neg_integer()|0,Struct)}.
-opaque(iter()::gb_trees:iter()|nil).

-spec(new() -> set(_)).
new() -> new(0,gb_trees:empty()).

-spec(new(non_neg_integer(),Struct) -> set(Struct)).
new(StartVer,Tree) ->
    #s_set{log = gb_trees:insert(-StartVer,Tree,gb_trees:empty())}.

-spec(append(any(),any(),non_neg_integer(), set(Struct)) -> set(Struct)).
append(Key,Val,Ver,Set) ->
    UpdateFun = fun(Last) -> gb_trees:enter(Key,Val,Last) end,
    add_new_version(Ver,Set,UpdateFun).

remove(Key, Ver,Set) ->
    UpdateFun = fun(Last) -> gb_trees:delete_any(Key,Last) end,
    add_new_version(Ver,Set,UpdateFun).

-spec update(non_neg_integer(),fun((Struct) -> Struct),set(Struct)) -> set(Struct).
update(Ver,UpdateFun,Set) ->
    add_new_version(Ver,Set,UpdateFun).

-spec get_at(non_neg_integer(),set(_)) -> gb_trees:iter().
get_at(Ver,Set) when Ver >= 0 ->
    iterator_from(Ver,0,Set).

-spec iterator_from(non_neg_integer(),non_neg_integer(),set(_)) -> gb_trees:iter().
iterator_from(Ver,Offset,Set) when Ver >= 0 ->
    TreeAtVer = get_tree_at(Ver,Set),
    gb_trees:iterator_from(Offset,TreeAtVer).

%% ===========================================================================
%%
%%
%% Iterator operations.
%% @todo: Move to_list, next and take to a generic lazy lists implementation
%%
%% ===========================================================================

-spec to_list(iter()) -> [].
to_list(Iter) ->
    to_list(Iter,[]).

to_list(Iter,Acc) ->
    case next(Iter) of
        {_,Val,Iter1} -> to_list(Iter1,[Val|Acc]);
        none -> Acc
    end.

next(Iter) -> gb_trees:next(Iter).

-spec take(non_neg_integer(),iter()) -> {[any()],iter()}.
take(Num,Iter) when Num >= 0 -> take(Num,Iter,[]).

take(0,Iter,Acc)   -> {Acc,Iter};
take(_Num,nil,Acc) -> {Acc,nil};
take(Num,Iter,Acc) ->
    case gb_trees:next(Iter) of
        none -> {Acc,nil};
        {_,Val,Iter1} -> take(Num-1,Iter1,[Val|Acc])
    end.


%% ===========================================================================
%% Iterator operations.
%% ===========================================================================

add_new_version(Ver,Set = #s_set{log = Log},Fun) when is_integer(Ver), Ver >= 0 ->
    {LastVer,Last} = gb_trees:smallest(Log),
    case LastVer =< -Ver of
        true -> error({invalid_ver, LastVer, Ver});
        false -> ok
    end,
    Next = Fun(Last),
    Set#s_set{log = gb_trees:insert(-Ver,Next,Log)}.

-spec get_tree_at(non_neg_integer(),set(Struct)) -> gb_trees:tree(Struct).
get_tree_at(Ver,#s_set{log = Log}) ->
    Iter = gb_trees:iterator_from(-Ver,Log), %% the whole reason for storing negative Sequence numbers
    case gb_trees:next(Iter) of
        none ->
            {_,LastVal} = gb_trees:smallest(Log),
            LastVal;
        {_,Val,_} -> Val
    end.

-spec(truncate(non_neg_integer(), set(Struct)) -> set(Struct)).
truncate(Ver,Set = #s_set{log = Log}) ->
    %% @todo: OPTIMIZE!!!
    L = lists:takewhile(fun({Key,_}) -> Key =< -Ver end, gb_trees:to_list(Log)),
    T = gb_trees:from_orddict(L), %%lists:foldl(fun({Key,Val},T) -> gb_trees:insert(Key,Val,T) end,gb_trees:empty(),L),
    NewLog =
        case gb_trees:is_empty(T) of
            true ->
                {LastVer,LastVal} = gb_trees:smallest(Log),
                gb_trees:insert(LastVer,LastVal,T);
            false -> T
        end,
    Set#s_set{log = NewLog}.

size(#s_set{log=Log}) -> gb_trees:size(Log).
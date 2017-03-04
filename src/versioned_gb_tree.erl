%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Mar 2017 2:01 AM
%%%-------------------------------------------------------------------
-module(versioned_gb_tree).
-author("Kalin").

%% API
-export([append/4, remove/3, new/0, new/2, iterator_from/3, to_list/1, take/2]).

-type set(Key,Val):: versioned_set:set(gb_trees:tree(Key,Val)).
-opaque(iter(Key,Val)::gb_trees:iter(Key,Val)|nil).

-spec(new() -> set(_,_)).
new() -> new(0,gb_trees:empty()).

-spec(new(non_neg_integer(),gb_trees:tree(Key,Val)) -> set(Key,Val)).
new(StartVer,Tree) ->
    versioned_set:new(StartVer,Tree).

-spec(append(Key,Val,non_neg_integer(),set(Key,Val)) -> set(Key,Val)).
append(Key,Val,Ver,Set) ->
    UpdateFun = fun(Last) -> gb_trees:enter(Key,Val,Last) end,
    versioned_set:update(Ver,UpdateFun,Set).

-spec(remove(Key,non_neg_integer(),set(Key,Val)) -> set(Key,Val)).
remove(Key,Ver,Set) ->
    UpdateFun = fun(Last) -> gb_trees:delete_any(Key,Last) end,
    versioned_set:update(Ver,UpdateFun,Set).

-spec iterator_from(non_neg_integer(),non_neg_integer(),set(Key,Val)) -> iter(Key,Val).
iterator_from(Ver,Offset,Set) when Ver >= 0 ->
    TreeAtVer = versioned_set:get_tree_at(Ver,Set),
    gb_trees:iterator_from(Offset,TreeAtVer).


-spec to_list(iter(_,Val)) -> [Val].
to_list(Iter) ->
    to_list(Iter,[]).

to_list(Iter,Acc) ->
    case next(Iter) of
        {_,Val,Iter1} -> to_list(Iter1,[Val|Acc]);
        none -> Acc
    end.

next(Iter) -> gb_trees:next(Iter).

-spec take(non_neg_integer(),iter(Key,Val)) -> {[Val],iter(Key,Val)}.
take(Num,Iter) when Num >= 0 -> take(Num,Iter,[]).

take(0,Iter,Acc)   -> {Acc,Iter};
take(_Num,nil,Acc) -> {Acc,nil};
take(Num,Iter,Acc) ->
    case gb_trees:next(Iter) of
        none -> {Acc,nil};
        {_,Val,Iter1} -> take(Num-1,Iter1,[Val|Acc])
    end.


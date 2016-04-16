%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Maintains different versions of a dictionary based on sequence Key.
%%% A memmory optimization so we can keep different lists of retained messages per
%%% Implemented in a NAIVE way right now - by using gb_trees with negative keys.
%%% @end
%%% Created : 30. Mar 2016 10:32 PM
%%%-------------------------------------------------------------------
-module(shared_set).
-author("Kalin").

%% API
-export([new/0, new/2, append/4, get_at/2, iterator_from/3, truncate/2, size/1, remove/3, take/2]).

-record(s_set,{
    log :: gb_trees:tree(non_neg_integer(),dict:dict())
}).

-type(set()::#s_set{}).

new() -> new(0,gb_trees:empty()).

-spec(new(non_neg_integer(),dict:dict()) -> set()).
new(StartSeq,Dict) ->
    #s_set{log = gb_trees:insert(-StartSeq,Dict,gb_trees:empty())}.

-spec(append(any(),any(),non_neg_integer(), set()) -> set()).
append(Key,Val,Seq,Set = #s_set{log = Log}) when is_integer(Seq),Seq > 0 ->
    {LastSeq,Last} = gb_trees:smallest(Log),
    case LastSeq =< -Seq  of
        true -> error({invalid_seq,LastSeq,Seq});
        false -> ok
    end,
    Next = gb_trees:enter(Key,Val,Last),
    Set#s_set{log = gb_trees:insert(-Seq,Next,Log)}.

remove(Key,Seq,Set = #s_set{log = Log}) when is_integer(Seq),Seq > 0 ->
    {LastSeq,Last} = gb_trees:smallest(Log),
    case LastSeq =< -Seq  of
        true -> error({invalid_seq,LastSeq,Seq});
        false -> ok
    end,
    Next = gb_trees:delete_any(Key,Last),
    Set#s_set{log = gb_trees:insert(-Seq,Next,Log)}.

-spec(get_at(non_neg_integer(),set()) -> []).
get_at(Seq,Set) when Seq > 0 ->
    iterator_from(Seq,0,Set).

-spec(iterator_from(non_neg_integer(),non_neg_integer(),set()) -> []).
iterator_from(Seq,Offset,Set) when Seq > 0 ->
    gb_trees:iterator_from(Offset,get_tree_at(Seq,Set)).

%%-spec(next(gb_trees:iter(Key,Val)) -> none | {Val,gb_trees:iter(Key,Val)}).
%%next(Iter) ->
%%    case gb_trees:next(Iter) of
%%        none -> none;
%%        {_,Val,Iter1} -> {Val,Iter1}
%%    end.

take(Num,Iter) when Num >= 0->
    take(Num,Iter,[]).

take(0,Iter,Acc) ->
    {Acc,Iter};

take(Num,Iter,Acc) ->
    case gb_trees:next(Iter) of
        none -> {Acc,nil};
        {_,Val,Iter1} -> take(Num-1,[Val|Acc],Iter1)
    end.

get_tree_at(Seq,#s_set{log = Log}) ->
    Iter = gb_trees:iterator_from(-Seq,Log), %% the whole reason for storing negative Sequence numbers
    case gb_trees:next(Iter) of
        none -> gb_trees:empty();
        {_,Val,_} -> Val
    end.

-spec(truncate(non_neg_integer(), set()) -> set()).
truncate(Seq,Set = #s_set{log = Log}) ->
    %% @todo: OPTIMIZE!!!
    L = lists:takewhile(fun({Key,_}) -> Key =< -Seq end, gb_trees:to_list(Log)),
    T = lists:foldl(fun({Key,Val},T) -> gb_trees:insert(Key,Val,T) end,gb_trees:empty(),L),
    Set#s_set{log = T}.


size(#s_set{log=Log}) -> gb_trees:size(Log).
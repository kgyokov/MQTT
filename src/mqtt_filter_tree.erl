%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 08. Mar 2017 11:50 PM
%%%-------------------------------------------------------------------
-module(mqtt_filter_tree).
-author("Kalin").

%% API
-export([]).

-define('|'(H,T),maybe_cons(H,T)).

-type branches()::any().
-type val()::any().
-type measure()::any().

-type node():: {Spec::'#'|'/'()|'+'()|txt(),
                Val::val(),Ms::measure()}.

-type node():: {Spec::'#'|'/'()|'+'()|txt(),
    Val::val(),Ms::measure()}.

%%-record('#',{val :: any()}).
-record('+',{'/' :: {'/'(),_,_}|nil}).
-record(txt,{'/' :: {'/'(),_,_}|nil, text::binary()}).
-record('/',{'/' :: {'/'(),_,_}|nil, '#'::{'#',_,_}|nil, branches :: branches()}).


get_vals_for_filter(RawPath,Tree) ->
    LPath = get_lense_from_raw_path(RawPath),
    Node = view(LPath)(Tree),
    view(innerval())(Node).

get_vals_for_matching_filters(RawPath,Tree) ->
    LPath = get_lense_from_raw_path(RawPath),
    view(LPath)(Tree).

update_raw_path(RawPath,Tree,Fun) ->
    LPath = get_lense_from_raw_path(RawPath),
    update(LPath,Fun)(Tree).

get_lense_from_raw_path(RawPath) ->
    LenseList = map_raw_to_lenses(RawPath),
    PathLenses = lists:foldr(fun(Lense,Acc) -> [children(),Lense|Acc] end, [innerval()], LenseList),
    compose(PathLenses).

%%
%% LENSES (If that's the appropriate name?!. Probably not)
%%

lense('/','#')       -> {fun({'/',_,Hash,_}) -> Hash end,
                         fun({'/',Plus,_,Txts},Hash)  -> {'/',Plus,Hash,Txts} end};
lense('/','+')       -> {fun({'/',Plus,_,_}) -> Plus end,
                         fun({'/',_,Hash,Txts},Plus) -> {'/',Plus,Hash,Txts} end};
lense('/',<<Txt>>)   -> {fun({'/',_,Txts}) -> in_dict_or_nil(Txt,Txts)  end,
                         fun({'/',Plus,Txts},Node)  -> {'/',Plus, store_if_not_nil(Txt,Node,Txts)} end};
lense('+','/')       -> {fun({'+',Slash}) -> Slash end,
                         fun({'+',_},Slash) -> {'+',Slash} end};
lense(<<Txt>>,'/')   -> {fun({txt,Slash})  -> Slash end,
                         fun({txt,_},Slash) -> {txt,Slash} end};
lense(root,'/')      -> {fun({root,Slash,_}) -> Slash end,
                         fun({root,_,Hash},Slash) -> {root,Slash,Hash} end};
lense(root,'#')      -> {fun({root,_,Hash}) -> Hash end,
                         fun({root,Slash,_},Hash) -> {root,Slash,Hash} end}.


default('#')       -> {'#',nil};
default('+')       -> {'+',nil};
default(<<_Txt>>)   -> {'txt',nil};
default('/')       -> {'/',nil,nil}.

children() -> {fun({Children,_}) -> Children end, fun(Val,{_,InnerVal}) -> {Val,InnerVal} end}.
innerval() -> {fun({_,InnerVal}) -> InnerVal end, fun(Val,{Children,_}) -> {Children,Val} end}.


in_dict_or_nil(Txt,Txts) ->
    case dict:find(Txt,Txts) of
        {ok,Node} -> Node;
        error -> nil
    end.

store_if_not_nil(nil,_,Txts) -> Txts;
store_if_not_nil(Txt,Node,Txts) -> dict:store(Txt,Node,Txts).

map_raw_to_lenses(Path) ->
    {Lenses,_} = lists:mapfoldl(fun(Seg,PrevSeg) -> {lense(PrevSeg,Seg),Seg} end,root,Path),
    Defaults = lists:map(fun default/1,Path),
    lists:zipwith(fun wrap_view_in_nil_check/2,Lenses,Defaults).



mappend_txt(A,B) -> error(not_implemented).

mappend(nil,A) -> A;
mappend(A,nil) -> A;

mappend({'/',Plus1,Hash1,Txt1},{'/',Plus2,Hash2,Txt2}) ->
    {'/',mappend(Plus1,Plus2),mappend(Hash1,Hash2),mappend_txt(Txt1,Txt2)};
mappend({'+',Slash1},{'+',Slash2}) ->
    {'+',mappend(Slash1,Slash2)};
mappend({txt,Slash1,Hash1},{txt,Slash2,Hash2}) ->
    {txt,mappend(Slash1,Slash2),mappend(Hash1,Hash2)};
mappend({root,Slash1,Hash1},{'root',Slash2,Hash2}) ->
    {'root',mappend(Slash1,Slash2),mappend(Hash1,Hash2)}.


%% ====================================================================================
%%
%% EXTENDING LENSE IMPLEMENTATION
%%
%% ====================================================================================


lense2('/',<<Txt>>)   -> {fun({'/',Plus,Hash,Txts},S) -> {[Plus,{dict,Txt,in_dict_or_nil(Txt,Txts)}],[Hash|S]} end,
    fun({'/',_,Hash,Txts},[Plus,{dict,Txt,Node}|T]) ->
        {{'/',Plus,Hash,store_if_not_nil(Txt,Node,Txts)},T}  end};

lense2('+','/')       -> {fun({'+',Slash},S)  -> {[Slash],S} end,
    fun({'+',_},[Slash|T])  -> {{'+',Slash},T} end};

lense2(<<Txt>>,'/')   -> {fun({txt,Slash},S)  -> {[Slash],S} end,
    fun({txt,_},[Slash|T])  -> {{txt,Slash},T} end};

lense2(root,'/')      -> {fun({root,Slash,_},S) -> {[Slash],S} end,
    fun({root,_,Hash},[Slash|T]) -> {{root,Slash,Hash},T} end};

lense2(root,'#')      -> {fun({root,Slash,Hash},S) -> {[Slash,Hash],S} end,
    fun({root,_,_},[Slash,Hash|T]) -> {{root,Slash,Hash},T} end};

lense2(_,nil)         -> {fun(N,S) -> {[],[N|S]}  end,
    fun(N,T) -> {N,T} end}.


filter_lense2_step({[{Node,[HL|TLs]}|TStack],S}) ->
    {Nodes,S1} = HL(Node,S),
    Stack2 = lists:mapfoldl(fun(Node2,Stack1) -> [{Node2,TLs}|Stack1] end,TStack,Nodes),
    {Stack2,S1};

filter_lense2_step({[{Node,[]}|TStack],S}) ->
    {TStack,[Node|S]};

filter_lense2_step({[],S}) -> {[],S}.

compose_view_funs2(ViewFuns) ->
    fun(Root,S) ->
        Stack = [{Root,ViewFuns}],
        compose_view_funs2_r({Stack,S})
    end.

compose_view_funs2_r({[],S}) -> {[],S};
compose_view_funs2_r(R)      -> compose_view_funs2_r(filter_lense2_step(R)).


%% ===========================================================================================
%%
%%  GENERIC LENSE(Maybe??) IMPLEMENTATION
%% ===========================================================================================

%%compose_lense2(L1,L2) ->
%%    fun(Node,T) ->
%%        T1 = lense2(Node,T),
%%        fold2(T1)
%%    end.


%% Applicative's sequenceA
%%fold_lenses({GetSeq,SetSeq}) ->
%%    {fun(Arg) -> lists:foldl(fun(Fun,Acc) -> Fun(Acc) end,Arg,GetSeq) end,
%%     fun(Arg) -> lists:foldr(fun(Fun,Acc) -> Fun(Acc) end,Arg,SetSeq) end}.


maybe_cons(nil,T) -> T;
maybe_cons(H,T) -> [H|T].

fold_path_lenses2(ViewFuns) ->
    fun(Node,T,S) -> lists:foldl(
        fun(ViewFun,{Level,Acc}) ->
            lists:foldl(ViewFun,{[],Acc},Level)
        end,{T,S},ViewFuns)
    end.

fold2(ViewFun,Acc,[]) -> Acc;

fold2(ViewFun,Acc,[H|T]) ->
    Acc1 = ViewFun(H,Acc),
    fold2(T,ViewFun,Acc1).


view({FunView,_}) -> FunView.
set(Lense) -> update(Lense, fun(X)-> X end).
update({FunView,FunSet}, Fun) -> fun(Val) -> FunSet(Fun(FunView(Val))) end.


%% Wrap function in a nil check
default_or_fun(FunView,Default) ->
    fun(ActualVal) ->
        case ActualVal of
            nil -> FunView(Default);
            _ -> FunView(ActualVal)
        end
    end.

compose(Lenses) ->
    GetSeq = [FunView || {FunView,_ } <- Lenses],
    SetSeq = [FunSet || {_,FunSet} <- Lenses],
    {fun(Arg) -> lists:foldl(fun(Fun,Acc) -> Fun(Acc) end,Arg,GetSeq) end,
     fun(Arg) -> lists:foldr(fun(Fun,Acc) -> Fun(Acc) end,Arg,SetSeq) end}.

wrap_view_in_nil_check({FunView,FunSet},Default) ->
    {default_or_fun(FunView,Default),FunSet}.


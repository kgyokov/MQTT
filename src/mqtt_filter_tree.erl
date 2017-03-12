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
-export([next_filter_match/1, get_vals_for_filter/2, get_iterator_for_matching_filters/2,
    update_raw_filter/3, mappend/2, new/0]).

-define('|'(H,T),maybe_cons(H,T)).

-type branches()::any().
-type val()::any().
-type measure()::any().

%%-type nodet():: {Spec::'#'|'/'()|'+'()|txt(),
%%                Val::val(),Ms::measure()}.
%%
%%%%-type nodet():: {Spec::'#'|'/'()|'+'()|txt(),
%%%%    Val::val(),Ms::measure()}.
%%
%%%%-record('#',{val :: any()}).
%%-record('+',{'/' :: {'/'(),_,_}|nil}).
%%-record(txt,{'/' :: {'/'(),_,_}|nil, text::binary()}).
%%-record('/',{'/' :: {'/'(),_,_}|nil, '#'::{'#',_,_}|nil, branches :: branches()}).

new() ->  {{root,nil,nil},nil}. %nil. %{{root,nil,nil},nil}.

get_vals_for_filter(RawFilter,Tree) ->
    LPath = get_lense_from_raw_path(RawFilter),
    (view(LPath))(Tree).

get_iterator_for_matching_filters(RawTopic,Tree) ->
    NPath = get_nexts_from_raw_path(RawTopic),
    get_filter_match_iterator1(NPath,Tree).

update_raw_filter(RawFilter,Tree,Fun) ->
    LPath = get_lense_from_raw_path(RawFilter),
    (update(LPath,Fun))(Tree).

get_nexts_from_raw_path(RawPath) ->
    NextList = map_raw_to_next_funs(RawPath),
    lists:foldr(fun(NextFun,Acc) -> [fun({Children,_}) ->  NextFun(Children) end|Acc] end, [], NextList).

get_lense_from_raw_path(RawPath) ->
    GetChildren = map_raw_to_lenses(RawPath),
    Defaults = lists:map(fun default_if_nil/1,RawPath),
    InterspersedL = lists:zipwith(fun(DefaultIfNil,GetChildren) -> [children(),GetChildren,DefaultIfNil] end,Defaults,GetChildren),
    PathLenses = lists:flatten(InterspersedL++[innerval()]),
    compose(PathLenses).

%%
%% LENSES (If that's the appropriate name?!. Probably not)
%%

lense('/','#')       -> {fun({'/',_,Hash,_}) -> Hash end,
                         fun({'/',Plus,_,Txts},Hash)  -> {'/',Plus,Hash,Txts} end};
lense('/','+')       -> {fun({'/',Plus,_,_}) -> Plus end,
                         fun({'/',_,Hash,Txts},Plus) -> {'/',Plus,Hash,Txts} end};
lense('/',<<Txt>>)   -> {fun({'/',_,_,Txts}) -> in_dict_or_nil(Txt,Txts)  end,
                         fun({'/',Plus,Hash,Txts},Node)  -> {'/',Plus,Hash,store_if_not_nil(Txt,Node,Txts)} end};
lense('+','/')       -> {fun({'+',Slash}) -> Slash end,
                         fun({'+',_},Slash) -> {'+',Slash} end};
lense(<<_Txt>>,'/')  -> {fun({txt,Slash})  -> Slash end,
                         fun({txt,_},Slash) -> {txt,Slash} end};
lense(root,'/')      -> {fun({root,Slash,_}) -> Slash end,
                         fun({root,_,Hash},Slash) -> {root,Slash,Hash} end};
lense(root,'#')      -> {fun({root,_,Hash}) -> Hash end,
                         fun({root,Slash,_},Hash) -> {root,Slash,Hash} end}.


default_if_nil(Type)  -> {fun(nil) -> default(Type); (A) -> A end,
                          fun(_,A) -> A end}.

default('root')    -> {{root,nil,nil},nil};
default('#')       -> {{'#',nil},nil};
default('+')       -> {{'+',nil},nil};
default(<<_Txt>>)  -> {{'txt',nil},nil};
default('/')       -> {{'/',nil,nil,dict:new()},nil}.

children() -> {fun({Children,_}) -> Children end, fun({_,InnerVal},Children) -> {Children,InnerVal} end}.
innerval() -> {fun({_,InnerVal}) -> InnerVal end, fun({Children,_},InnerVal) -> {Children,InnerVal} end}.


in_dict_or_nil(Txt,Txts) ->
    case dict:find(Txt,Txts) of
        {ok,Node} -> Node;
        error -> nil
    end.

store_if_not_nil(nil,_,Txts) -> Txts;
store_if_not_nil(Txt,Node,Txts) -> dict:store(Txt,Node,Txts).

map_raw_to_lenses(Path) ->
    {Lenses,_} = lists:mapfoldl(fun(Seg,PrevSeg) -> {lense(PrevSeg,Seg),Seg} end,root,Path),
    Lenses.

mappend_txt(_,_) -> error(not_implemented).

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

%%todo: Maybe don't add txt to the returned values
next('/',<<Txt>>)  -> fun({'/',Plus,Hash,Txts}) -> {[Plus,in_dict_or_nil(Txt,Txts)],[Hash]} end;
next('+','/')      -> fun({'+',Slash})  -> {[Slash],[]} end;
next(<<_Txt>>,'/') -> fun({txt,Slash})  -> {[Slash],[]} end;
next(root,'/')     -> fun({root,Slash,_}) -> {[Slash],[]} end;
next(_,nil)        -> fun(N) -> {[],[N]}  end;
next(nil,_)        -> fun(_) -> {[],[]}  end.

map_raw_to_next_funs(Path) ->
    {Funs,_} = lists:mapfoldl(fun(Seg,PrevSeg) -> {next(PrevSeg,Seg),Seg} end,root,Path),
    Funs.

get_filter_match_iterator1(NextFuns,Root) -> {[],[{Root,NextFuns}]}.

next_filter_node_match([]) -> {[],[]};
next_filter_node_match([{Node,[]}|TStack]) -> {[Node],TStack};
next_filter_node_match([{nil,_}|TStack]) -> next_filter_node_match(TStack);
next_filter_node_match([{Node,[HPath|TPath]}|TStack]) ->
    {NewNodes,Solutions} = HPath(Node),
    Stack1 = [{N,TPath} || N <- NewNodes,N =/= nil] ++ TStack,
    Solutions1 = [Sol|| Sol <- Solutions, Sol =/= nil],
    case Solutions1 of
        [] -> next_filter_node_match(Stack1);
        _ -> {Solutions1,Stack1}
    end.


next_filter_match({[{_,Val}|TSol],Stack}) -> {Val,{TSol,Stack}};

next_filter_match({[],Stack}) ->
    {Solutions,Stack1} = next_filter_node_match(Stack),
    case Solutions of
        [] -> nil;
        [{_,Val}|TSol] -> {Val,{TSol,Stack1}}
    end.



%%lense2('/',<<Txt>>)   -> {fun({'/',Plus,Hash,Txts},S) -> {[Plus,{dict,Txt,in_dict_or_nil(Txt,Txts)}],[Hash|S]} end,
%%    fun({'/',_,Hash,Txts},[Plus,{dict,Txt,Node}|T]) ->
%%        {{'/',Plus,Hash,store_if_not_nil(Txt,Node,Txts)},T}  end};
%%
%%lense2('+','/')       -> {fun({'+',Slash},S)  -> {[Slash],S} end,
%%    fun({'+',_},[Slash|T])  -> {{'+',Slash},T} end};
%%
%%lense2(<<Txt>>,'/')   -> {fun({txt,Slash},S)  -> {[Slash],S} end,
%%    fun({txt,_},[Slash|T])  -> {{txt,Slash},T} end};
%%
%%lense2(root,'/')      -> {fun({root,Slash,_},S) -> {[Slash],S} end,
%%    fun({root,_,Hash},[Slash|T]) -> {{root,Slash,Hash},T} end};
%%
%%lense2(root,'#')      -> {fun({root,Slash,Hash},S) -> {[Slash,Hash],S} end,
%%    fun({root,_,_},[Slash,Hash|T]) -> {{root,Slash,Hash},T} end};
%%
%%lense2(_,nil)         -> {fun(N,S) -> {[],[N|S]}  end,
%%    fun(N,T) -> {N,T} end}.
%%
%%
%%filter_lense2_step({LStack,[HStack = {Node,[HL|TLs]}|TStack],S}) ->
%%    {Nodes,S1} = HL(Node,S),
%%    TStack2 = lists:mapfoldl(fun(Node2,TStack1) -> [{Node2,TLs}|TStack1] end,TStack,Nodes),
%%    {[HStack|LStack],TStack2,S1};
%%
%%filter_lense2_step({LStack,[HStack = {Node,[]}|TStack],S}) ->
%%    {[HStack|LStack],TStack,[Node|S]};
%%
%%filter_lense2_step(R = {_,[],_}) -> R.
%%
%%filter_lense2_step_set(Step) ->
%%    Node1 = HL(Node,S),
%%
%%
%%
%%compose_view_funs2(ViewFuns) ->
%%    fun(Root,S) ->
%%        Stack = [{Root,ViewFuns}],
%%        compose_view_funs2_r({[],Stack,S})
%%    end.
%%
%%compose_view_funs2_r(R = {_,[],_}) -> R;
%%compose_view_funs2_r(R)      -> compose_view_funs2_r(filter_lense2_step(R)).

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

%%fold_path_lenses2(ViewFuns) ->
%%    fun(Node,T,S) -> lists:foldl(
%%        fun(ViewFun,{Level,Acc}) ->
%%            lists:foldl(ViewFun,{[],Acc},Level)
%%        end,{T,S},ViewFuns)
%%    end.
%%
%%fold2(ViewFun,Acc,[]) -> Acc;
%%
%%fold2(ViewFun,Acc,[H|T]) ->
%%    Acc1 = ViewFun(H,Acc),
%%    fold2(T,ViewFun,Acc1).


view({FunView,_}) -> fun(Val) ->
                        {_,Acc} = FunView(Val),
                        Acc
                     end.
set(Lense) -> update(Lense, fun(X)-> X end).
update({FunView,FunSet}, Fun) ->
    fun(Val) ->
        {Stack,Acc} = FunView(Val),
        FunSet({Stack,Fun(Acc)})
    end.



%%%% Wrap function in a nil check
%%default_or_fun(FunView,Default) ->
%%    fun(ActualVal) ->
%%        case ActualVal of
%%            nil -> FunView(Default);
%%            _ -> FunView(ActualVal)
%%        end
%%    end.


%%%% Wrap function in a nil check
%%default_or_fun(FunView,Default) ->
%%    fun(ActualVal) ->
%%        case ActualVal of
%%            nil -> FunView((children())({Default,nil}));
%%            _ -> FunView(ActualVal)
%%        end
%%    end.

compose(Lenses) ->
    GetSeg = [FunView || {FunView,_ } <- Lenses],
    SetSeg = [FunSet || {_,FunSet} <- Lenses],
    {fun(Val) -> lists:mapfoldl(fun(Fun,Acc) -> {Acc,Fun(Acc)} end,Val,GetSeg) end,
     fun({Stack,Val}) ->
         StackWSeg = lists:zip(Stack,SetSeg),
         lists:foldr(fun({StackVal,Fun},Acc) -> Fun(StackVal,Acc) end,Val,StackWSeg) end}.



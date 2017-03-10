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


get_vals_for(RawPath,Tree) ->
    LensesPath = map_raw_to_lenses(RawPath),
    LPath = fold_path_to_lense(LensesPath),
    view(LPath)(Tree).

get_node_val({Children,Val}) -> Val.

%%plus (nil,                   T,      Fun) -> plus({'+',nil,nil,nil},T,Fun);
%%plus ({'+',Slash,Val,Ms},    ['/'],  Fun) -> {'+',Slash,Fun(Val),Ms};
%%plus ({'+',Slash,Val,Ms},    ['/'|T],Fun) -> {'+',slash(Slash,T,Fun),Val,Ms}.
%%
%%txt  (nil,                   T,      Fun) -> txt({txt,nil,nil,nil},T,Fun);
%%txt  ({txt,Slash,Val,Ms},    ['/'],  Fun) -> {txt,Slash,Fun(Val),Ms};
%%txt  ({txt,Slash,Val,Ms},    ['/'|T],Fun) -> {txt,slash(Slash,T,Fun),Val,Ms}.
%%
%%slash(nil,                    T      ,Fun) -> slash({'/',nil,dict:new(),nil,nil},T,Fun);
%%slash({'/',Hash,Txts,Val,Ms}, [],     Fun) -> {'/',Hash,Txts,Fun(Val),Ms};
%%slash({'/',Hash,Txts,Val,Ms}, ['#']  ,Fun) -> {'/',Fun(Hash),Txts,Val,Ms};
%%slash({'/',Hash,Txts,Val,Ms}, [<<Text>>|T]  ,Fun) ->
%%    Branches1 =
%%        case dict:find(Text,Txts) of
%%            {ok,TextNode} ->
%%                dict:store(Text,txt(TextNode,T,Fun),Txts);
%%            error ->
%%                NewNode = {{txt,nil,Text},nil,nil},
%%                dict:store(Text,txt(NewNode,T,Fun),Txts)
%%        end,
%%    {'/',Hash,Branches1,Val,Ms}.


%%map_path('/','#')       -> fun({'#',X},Fun) -> {'#',Fun(X)} end;
%%map_path('/','+')       -> fun({'/',Plus,Branches},Fun) -> {'/',Fun(Plus),Branches} end;
%%map_path('/',<<Txt>>)   -> fun({'/',Plus,Branches},Fun) -> {'/',Fun(Plus),Branches} end;
%%map_path('/',nil)       -> fun({'/',Plus,Branches},Fun) -> {'/',Fun(Plus),Branches} end;
%%
%%map_path('+','/')       -> fun({'+',Slash},Fun) -> {'+',Fun(Slash)} end;
%%map_path('+',nil)       -> fun({'/',Plus,Branches}) -> Plus end;
%%
%%map_path(<<Txt>>,slash) -> fun({txt,Slash},Fun) -> {txt,Fun(Slash)} end;
%%map_path(<<Txt>>,nil)   -> ok;
%%map_path('#',nil) -> ok.


%%
%% LENSES (If that's the appropriate name?!. Probably not)
%%

lense('/','#')       -> {fun({'/',_,Hash,_}) -> Hash end,
                         fun({'/',Plus,_,Txts},Hash)  -> {'/',Plus,Hash,Txts} end};
lense('/','+')       -> {fun({'/',Plus,_,_}) -> Plus end,
                         fun({'/',_,Hash,Txts},Plus) -> {'/',Plus,Hash,Txts} end};
lense('/',<<Txt>>)   -> {fun({'/',_,Txts}) -> in_dict_or_nil(Txt,Txts)  end,
                         fun({'/',Plus,Txts},Node)  -> {'/',Plus,dict:store(Txt,Node,Txts)} end};
lense('+','/')       -> {fun({'+',Slash}) -> Slash end,
                         fun({'+',_},Slash) -> {'+',Slash} end};
lense(<<Txt>>,'/')   -> {fun({txt,Slash})  -> Slash end,
                         fun({txt,_},Slash) -> {txt,Slash} end};
lense(root,'/')      -> {fun({root,Slash,_}) -> Slash end,
                         fun({root,_,Hash},Slash) -> {root,Slash,Hash} end};
lense(root,'#')      -> {fun({root,_,Hash}) -> Hash end,
                         fun({root,Slash,_},Hash) -> {root,Slash,Hash} end}.

lense2('/','#')       -> {fun({'/',Plus,Hash,Txts},T) -> [Plus,Hash|T] end,
                          fun([Plus,Hash|T],{'/',_,_})  -> {{'/',Plus,Hash},T} end};
lense2('/','+')       -> {fun({'/',Plus,Txts},T) -> [Plus,Txts|T] end,
                          fun({'/',_,Hash},[Plus,Txts|T]) -> {'/',Plus,Hash} end};
lense2('/',<<Txt>>)   -> {fun({'/',_,_,Txt}) -> [Txt]  end,
                          fun(S,{'/',Plus,Txt})  -> {'/',Plus,Txt ++ Txt} end};
lense2('+','/')       -> {fun({'+',Slash}) -> [Slash] end,
                          fun(S,Val1,_) -> {'+',Val1} end};
lense2(<<Txt>>,'/')   -> {fun({txt,Slash})  -> [Slash] end,
                          fun(S,Val,_) -> {txt,Val} end};
lense2(root,'/')      -> {fun({root,Slash,_},T) -> [Slash|T] end,
                          fun({root,_,Hash},[Slash|T]) -> {{root,Slash,Hash},T} end};
lense2(root,'#')      -> {fun({root,Slash,Hash},T) -> [Slash,Hash|T] end,
                          fun({root,_,_},[Slash,Hash|T]) -> {{root,Slash,Hash},T} end}.

default('#')       -> {'#',nil};
default('+')       -> {'+',nil};
default(<<Txt>>)   -> {'txt',nil};
default('/')       -> {'/',nil,nil}.

in_dict_or_nil(Txt,Txts) ->
    case dict:find(Txt,Txts) of
        {ok,Node} -> Node;
        error -> nil
    end.


lense_children() -> {fun({Children,_}) -> Children end, fun(Val,{_,InnerVal}) -> {Val,InnerVal} end}.
lense_innerval() -> {fun({_,InnerVal}) -> InnerVal end, fun(Val,{Children,_}) -> {Children,Val} end}.

lense2_fold([H|T],LastSeg,Stack) ->
    Stack1 = [lense2(LastSeg,H)++Stack].


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

%% Applicative's sequenceA
fold_path_to_lense({GetSeq,SetSeq}) ->
    {fun(Arg) -> lists:foldl(fun(Fun,Acc) -> Fun(Acc) end,Arg,GetSeq) end,
     fun(Arg) -> lists:foldr(fun(Fun,Acc) -> Fun(Acc) end,Arg,SetSeq) end}.

map_raw_to_lenses(Path) ->
    {Lenses,_} = lists:mapfoldl(fun(Seg,PrevSeg) -> {lense(PrevSeg,Seg),Seg} end,root,Path),
    Defaults = lists:map(fun default/1,Path),
    lists:zipwith(fun wrap_view_in_nil_check/2,Lenses,Defaults).

wrap_view_in_nil_check({FunView,FunSet},Default) ->
    {default_or_fun(FunView,Default),FunSet}.






















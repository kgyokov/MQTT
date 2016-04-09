%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% Simple document based implementation of subscriptions
%%% @end
%%% Created : 09. Dec 2014 2:13 AM
%%%-------------------------------------------------------------------
-module(mqtt_sub_repo).
-author("Kalin").

-include_lib("mqtt_internal_msgs.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% API
-export([create_tables/2,
    wait_for_tables/0,
    save_sub/2,
    remove_sub/2,
    clear_sub_pid/2,
    get_matching_subs/1,
    unclaim_filter/2,
    claim_filter/2,
    load/1,
    get_subs/1]).

-ifdef(TEST).
    -export([clear_tables/0,delete_tables/0]).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-define(SUB_RECORD, mqtt_sub).
-define(SUB_REG_RECORD, mqtt_sub_reg).
-define(ALL_TABLES,[?SUB_RECORD,?SUB_REG_RECORD]).

-record(mqtt_sub,{
    id          ::{Filter   ::binary(),
                   ClientId ::client_id()},
    filter      ::binary(),
    client_id   ::client_id(),
    qos         ::qos(),
    pid         ::pid(),
    cseq = 1    ::non_neg_integer()
}).

-record(mqtt_sub_reg,{
    filter  ::binary(),
    pid     ::pid()
}).


-define(BASIC_TABLE_DEF(Nodes,Type),[
    {?PERSISTENCE, Nodes},
    {type,set},
    {attributes,record_info(fields,Type)}
]).

-define(DISTRIBUTED_DEF(NFragments,Nodes), (
    {frag_properties,
        {n_fragments,NFragments},
        {node_pool,Nodes}}
)).


%% ======================================================================
%% API
%% ======================================================================


%% @doc
%% Appends a new subscription OR replaces an existing one with a new QoS
%% @end

-spec save_sub(ClientId::client_id(),
    {
        Filter::binary(),
        QoS::qos(),
        Seq::integer(),
        ClientPid::pid()}
) ->
    {ok,Result::any()}.

save_sub(Filter,{ClientId,QoS,CSeq,ClientPid}) ->
    Fun =
        fun() ->
             R = #mqtt_sub{id = {Filter,ClientId},
                           filter = Filter,
                           client_id = ClientId,
                           cseq = CSeq,
                           pid = ClientPid,
                           qos = QoS},
            mnesia:write(R)
        end,
    mnesia_do(Fun).

clear_sub_pid(Filter,ClientId) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_RECORD,{Filter,ClientId},write) of
                [] ->
                    ok;
                [S]->
                    R = S#mqtt_sub{pid = undefined},
                    mnesia:write(R)
            end
        end,
    mnesia_do(Fun).


%% @doc
%% Removes a subscription
%%
%% @end

-spec remove_sub(Filter::binary(),ClientId::client_id()) -> ok.

remove_sub(Filter,ClientId) ->
    Fun =
        fun() ->
            mnesia:delete({?SUB_RECORD,{Filter,ClientId}})
        end,
    mnesia_do(Fun).



%% @doc
%% Locks a given Filter to a process and loads a list of subscriptions for that filter
%% @end

-spec(load(Filter::binary()) -> [{ClientId::client_id(),QoS::qos(),Seq::integer(),ClientPid::pid()}]).
load(Filter) ->
    Fun =
        fun() ->
            Q = qlc:q([{ClientId,QoS,CSeq,ClientPid}  ||
                        #mqtt_sub{filter = Filter,
                                  client_id = ClientId,
                                  qos = QoS,
                                  cseq = CSeq,
                                  pid = ClientPid} <- mnesia:table(?SUB_RECORD)]),
            qlc:e(Q)
        end,
    mnesia_do(Fun).

%% @doc
%% Gets the Pids of the Subs matching the given topic
%% @end
-spec get_matching_subs(Topic::topic()) -> [{Filter::binary(),Pid::pid()}].

get_matching_subs(Topic) ->
    Patterns = mqtt_topic:explode(Topic),
    Spec = [{
        #mqtt_sub_reg{filter = '$1', pid = '$2', _ = '_'},
        [{'=:=', '$1', P}],
        [['$1','$2']]
    } || P <- Patterns],
    [{F,P}|| [F,P]<- mnesia:dirty_select(?SUB_REG_RECORD,Spec)].


%% @doc
%% Registers a Pid that will handle the given filter
%% @end
claim_filter(Filter,Pid) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_REG_RECORD,Filter,write) of
                [] ->
                    ok = mnesia:write(#mqtt_sub_reg{filter = Filter,pid = Pid});
                [S = #mqtt_sub_reg{pid = OldPid}] when OldPid =/= Pid ->
                    ok = mnesia:write(S#mqtt_sub_reg{pid = Pid});
                _ -> ok
            end
        end,
    mnesia_do(Fun).

unclaim_filter(Filter,Pid) ->
    Fun =
        fun() ->
            case mnesia:read(?SUB_REG_RECORD,Filter,write) of
                [] -> ok;
                [S = #mqtt_sub_reg{pid = Pid}] ->
                    ok = mnesia:write(S#mqtt_sub_reg{pid = undefined});
                _ -> ok
            end
        end,
    mnesia_do(Fun).

get_subs(Filter) ->
    Spec = [{
        #mqtt_sub{filter = '$1', pid = '$2', _ = '_'},
        [{'=:=', '$1', Filter},{'=/=', '$2', undefined}],
        ['$2']
    }],
    case mnesia:dirty_select(?SUB_RECORD, Spec) of
        [] -> error;
        [Pid] -> {ok,Pid}
    end.


%% ======================================================================
%% PRIVATE Misc
%% ======================================================================

mnesia_do(Fun) ->
    mnesia:activity(transaction,Fun,[],mnesia_frag).

%% ======================================================================
%% Table creation, cleanup, etc.
%% ======================================================================

-ifdef(TEST).

clear_tables() ->
    lists:foreach(fun clear_table/1,?ALL_TABLES).

delete_tables() ->
    lists:foreach(fun delete_table/1,?ALL_TABLES).

clear_table(Table) ->
    case mnesia:clear_table(Table) of
        {atomic,ok}                 -> ok;
        {aborted,{no_exists,Table}} -> ok
    end.

delete_table(Table) ->
    case mnesia:delete_table(Table) of
        {atomic,ok}                 -> ok;
        {aborted,{no_exists,Table}} -> ok
    end.

-endif.

wait_for_tables() ->
    ok = mnesia:wait_for_tables(?ALL_TABLES,5000).

%% @doc
%% Creates the mnesia tables. To be called only once.
%% @end

create_tables([],NFragments) ->
    create_tables([node()],NFragments);

create_tables(Nodes,NFragments) ->
    OtherProps =
        if NFragments < 2 -> [];
            true -> [?DISTRIBUTED_DEF(NFragments,Nodes)]
        end,
    SubDef      = OtherProps ++ ?BASIC_TABLE_DEF(Nodes,?SUB_RECORD),
    SubRegDef   = OtherProps ++ ?BASIC_TABLE_DEF(Nodes,?SUB_REG_RECORD),
    TableList = [{?SUB_RECORD,SubDef},{?SUB_REG_RECORD,SubRegDef}],
    lists:foreach(fun maybe_create_new_table/1,TableList).

maybe_create_new_table({Table,AllProps}) ->
    case mnesia:create_table(Table,AllProps) of
        {atomic,ok}                      -> ok;
        {aborted,{already_exists,Table}} -> ok
    end.
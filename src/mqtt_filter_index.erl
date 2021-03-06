%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%  Used to keep a list of existing topics per filter
%%% This way we can qucikly grab a list of topics for a given filter whenever it gets a new subscriber
%%% (and consequently obtain the retained messages)
%%% @end
%%% Created : 07. Apr 2015 10:05 PM
%%%-------------------------------------------------------------------
-module(mqtt_filter_index).
-author("Kalin").

%% API
-export([add_topic/1, get_matching_topics/1, wait_for_tables/0, create_tables/2]).

-record(mqtt_filter_idx,{
    pk,
    filter,
    topic,
    clock, %% @todo: Use logical clock?
    timestamp
}).

-define(IDX_TABLE, mqtt_filter_idx).

-ifdef(TEST).
    -export([clear_tables/0,delete_tables/0]).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-define(BASIC_TABLE_DEF(Nodes),[
    {?PERSISTENCE, Nodes},
    {type,bag},                          %% @todo: test 'bag' performance
    {attributes,record_info(fields,mqtt_filter_idx)},
    {index,[#mqtt_filter_idx.filter]}
]).


%% ======================================================================
%% API
%% ======================================================================
-spec(add_topic(Topic::binary()) ->
    term()).
add_topic(Topic) ->
    Filters = mqtt_topic:explode(Topic),
    %%Fun = fun() ->
        [begin
                Idx = #mqtt_filter_idx{
                    filter = Filter,
                    topic = Topic
                    %% clock = Clock,
                    %% timestamp = time()
                },
                mnesia:dirty_write(Idx)
         end
            || Filter <- Filters]
    %%end,
    %%mnesia_do(Fun)
.
-spec(get_matching_topics(Filters::[binary()]) ->
    [binary()]).
get_matching_topics(Filters) ->
    Ms = [{
            #mqtt_filter_idx{filter = '$1', topic = '$2', _ = '_'},
            [{'=:=','$1',Filter}],
            ['$2']
        }|| Filter <- Filters],
    mnesia:dirty_select(?IDX_TABLE,Ms).


%% ======================================================================
%% Table creation, cleanup, etc.
%% ======================================================================

create_tables(Nodes,_NFragments) ->
    Props = ?BASIC_TABLE_DEF(Nodes),
%%     Props = if NFragments < 2 ->
%%         DefaultProps;
%%                 true ->
%%                     [?DISTRIBUTED_DEF(NFragments,Nodes) | DefaultProps]
%%             end,

    case mnesia:create_table(?IDX_TABLE, Props) of
        {atomic, ok}                            -> ok;
        {aborted, {already_exists, ?IDX_TABLE}} -> ok
    end.

wait_for_tables() ->
    ok = mnesia:wait_for_tables([?IDX_TABLE],5000).

-ifdef(TEST).

clear_tables() ->
    {atomic,ok} = mnesia:clear_table(?IDX_TABLE).

delete_tables() ->
    {atomic,ok} = mnesia:delete_table(?IDX_TABLE).

-endif.
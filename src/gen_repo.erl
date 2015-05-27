%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Apr 2015 1:31 AM
%%%-------------------------------------------------------------------
-module(gen_repo).
-author("Kalin").

-ifdef(TEST).
    -define(PERSISTENCE, ram_copies).
-else.
    -define(PERSISTENCE, disc_copies).
-endif.

-define(BASIC_TABLE_DEF(Nodes,Type,Record),[
    {?PERSISTENCE, Nodes},
    {type,Type},
    {attributes,record_info(fields,Record)}
]).

-define(DISTRIBUTED_DEF(NFragments,Nodes), (
    {frag_properties,
     {n_fragments,NFragments},
     {node_pool,Nodes}}
)).

-callback wait_for_tables() -> term().
-callback create_tables(Nodes ::[node()],NFrag::non_neg_integer()) -> term().

-ifdef(TEST).
    -callback clear_tables() -> term().
    -callback delete_tables() -> term().
-endif.






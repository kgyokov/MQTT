%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Mar 2017 8:33 PM
%%%-------------------------------------------------------------------
-module(accumulator_gb_tree).
-author("Kalin").

-include("mqtt_internal_msgs.hrl").

-behavior(gen_accumulator).

%% API
-export([id/0, acc/2]).


id() -> gb_trees:empty().

acc(#packet{retain = true,topic = Topic,content = <<>>},Acc) ->  gb_trees:delete_any(Topic,Acc);
acc(#packet{retain = true,topic = Topic} = Packet,Acc)       ->  gb_trees:enter(Topic,Packet,Acc);
acc(_,Acc)                                                   ->  Acc.
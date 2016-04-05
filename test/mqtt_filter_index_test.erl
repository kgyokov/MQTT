%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mar 2015 10:09 PM
%%%-------------------------------------------------------------------
-module(mqtt_filter_index_test).
-author("Kalin").

-compile(export_all).

-include("test_utils.hrl").


setup()->
    Nodes = [node()],
    mnesia:create_schema(Nodes),
    application:start(mnesia),
    mqtt_filter_index:create_tables(Nodes,1),
    mqtt_filter_index:wait_for_tables().

teardown()->
    mqtt_filter_index:delete_tables().

filter_index_test_() ->
    {foreach,
     fun() -> setup() end,
     fun(_) -> teardown() end,
     %%{inparallel,
     [
         {spawn,
          fun() ->
              ?lists_are_equal([],
                               mqtt_filter_index:get_matching_topics([<<"/A/1">>]))
          end
         },
         {spawn,
          fun() ->
              mqtt_filter_index:add_topic( <<"/A/1">>),
              ?lists_are_equal([<<"/A/1">>],
                               mqtt_filter_index:get_matching_topics([<<"/A/1">>]))
          end
         },
         {spawn,
          fun() ->
              mqtt_filter_index:add_topic( <<"/A/1">>),
              mqtt_filter_index:add_topic( <<"/B/1">>),
              ?lists_are_equal([<<"/A/1">>],
                               mqtt_filter_index:get_matching_topics([<<"/A/+">>]))
          end
         },
         {spawn,
          fun() ->
              mqtt_filter_index:add_topic( <<"/A/1">>),
              mqtt_filter_index:add_topic( <<"/B/1">>),
              ?lists_are_equal([<<"/A/1">>,<<"/B/1">>],
                               mqtt_filter_index:get_matching_topics([<<"/#">>]))
          end
         },
         {spawn,
          fun() ->
              mqtt_filter_index:add_topic( <<"/A/1">>),
              mqtt_filter_index:add_topic( <<"/B/1">>),
              ?lists_are_equal([],
                               mqtt_filter_index:get_matching_topics([<<"/+/+/+">>]))
          end
         },
         {spawn,
          fun() ->
              mqtt_filter_index:add_topic( <<"/A/1">>),
              mqtt_filter_index:add_topic( <<"/B/1">>),
              ?lists_are_equal([<<"/A/1">>, <<"/B/1">>],
                               mqtt_filter_index:get_matching_topics([<<"/A/+">>,<<"/B/+">>]))
          end
         },
         {spawn,
          fun() ->
              mqtt_filter_index:add_topic(<<"/A/1">>),
              mqtt_filter_index:add_topic(<<"/A/1">>),
              ?lists_are_equal([<<"/A/1">>],
                               mqtt_filter_index:get_matching_topics([<<"/A/+">>]))
          end
         }
     ]
     %%}
    }.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mar 2015 10:09 PM
%%%-------------------------------------------------------------------
-module(mqtt_sub_repo_test).
-author("Kalin").

-compile(export_all).

-include("test_utils.hrl").

setup()->
    Nodes = [node()],
    mnesia:create_schema(Nodes),
    application:start(mnesia),
    mqtt_sub_repo:create_tables(Nodes,1),
    mqtt_sub_repo:wait_for_tables().

teardown()->
    mqtt_sub_repo:delete_tables().

sub_persistence_test_() ->
        {foreach,
            fun() -> setup() end,
            fun(_) -> teardown() end,
            %%{inparallel,
                [
                    {spawn,
                        fun() ->
                            ?lists_are_equal([], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client1">>, <<"/A/+">>),
                            ?lists_are_equal([], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 2),
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            ?lists_are_equal([{<<"Client1">>, 1}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client1">>, <<"/A/+">>),
                            ?lists_are_equal([], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/1">>, 1),
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 2),
                            ?lists_are_equal([{<<"Client1">>, 2}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:save_sub(<<"Client2">>, <<"/+/1">>, 2),
                            ?lists_are_equal([{<<"Client1">>,1},{<<"Client2">>,2}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:save_sub(<<"Client2">>, <<"/+/1">>, 2),
                            ?lists_are_equal([{<<"Client1">>,1},{<<"Client2">>,2}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client1">>, <<"/A/+">>),

                            mqtt_sub_repo:save_sub(<<"Client2">>, <<"/+/1">>, 2),
                            ?lists_are_equal([{<<"Client2">>,2}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client2">>, <<"/+/1">>),
                            ?lists_are_equal([{<<"Client1">>,1}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },

                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>,  <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client2">>, <<"/+/1">>),
                            ?lists_are_equal([{<<"Client1">>,1}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client2">>, <<"/+/1">>),
                            ?lists_are_equal([{<<"Client1">>,1}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1),
                            mqtt_sub_repo:remove_sub(<<"Client2">>, <<"/+/1">>),
                            ?lists_are_equal([{<<"Client1">>,1}], mqtt_sub_repo:get_matches(<<"/A/1">>))
                        end
                    },
                    {spawn,
                         fun() ->
                             ?assertMatch({ok,new},      mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1)),
                             ?assertMatch({ok,existing}, mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1)),
                             ?assertMatch({ok,new},      mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 2)),
                             ?assertMatch(ok,            mqtt_sub_repo:remove_sub(<<"Client1">>, <<"/A/+">>)),
                             ?assertMatch({ok,new},      mqtt_sub_repo:save_sub(<<"Client1">>, <<"/A/+">>, 1))
                         end
                    }
                ]
            %%}
        }.

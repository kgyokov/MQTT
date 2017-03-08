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
-include("mqtt_internal_msgs.hrl").

setup()->
    Nodes = [node()],
    mnesia:create_schema(Nodes),
    application:start(mnesia),
    ok = mqtt_sub_repo:delete_tables(),
    ok = mqtt_sub_repo:create_tables(Nodes,1),
    ok = mqtt_sub_repo:wait_for_tables().

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
                            ?lists_are_equal([], mqtt_sub_repo:load(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Client = <<"Client1">>,
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:save_sub(Filter,{Client,?QOS_1,1,self()}),
                            mqtt_sub_repo:remove_sub(Filter,Client),
                            ?assertEqual([],mqtt_sub_repo:load(Filter))
                        end
                    },
                    {spawn,
                        fun() ->
                            Client = <<"Client1">>,
                            Filter = <<"/A/+">>,
                            ?assertEqual(ok,mqtt_sub_repo:remove_sub(Filter,Client)),
                            ?assertEqual([],mqtt_sub_repo:load(Filter))
                        end
                    },
                    {spawn,
                        fun() ->
                            Client = <<"Client1">>,
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:save_sub(Filter,{Client,?QOS_1,1,self()}),
                            mqtt_sub_repo:save_sub(Filter,{Client,?QOS_2,2,self()}),
                            ?assertEqual([{Client,?QOS_2,2,self()}], mqtt_sub_repo:load(Filter))
                        end
                    },
                    {spawn,
                        fun() ->
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            ?assertEqual([{Filter,self()}],mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            ?assertEqual([{Filter,self()}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            mqtt_sub_repo:unclaim_filter(Filter,self()),
                            ?assertEqual([{Filter,undefined}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            mqtt_sub_repo:unclaim_filter(Filter,spawn(fun() -> ok end)),
                            ?assertEqual([{Filter,self()}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            mqtt_sub_repo:unclaim_filter(Filter,self()),
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            ?assertEqual([{Filter,self()}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Filter = <<"/A/+">>,
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            mqtt_sub_repo:claim_filter(Filter,self()),
                            mqtt_sub_repo:unclaim_filter(Filter,self()),
                            ?assertEqual([{Filter,undefined}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            OtherPid = spawn(fun() -> ok end),
                            mqtt_sub_repo:claim_filter(<<"/A/1">>, self()),
                            mqtt_sub_repo:claim_filter(<<"/A/+">>, OtherPid),
                            ?lists_are_equal([{<<"/A/1">>, self()},{<<"/A/+">>, OtherPid}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            OtherPid = spawn(fun() -> ok end),
                            mqtt_sub_repo:claim_filter(<<"/+/1">>, self()),
                            mqtt_sub_repo:claim_filter(<<"/A/+">>, OtherPid),
                            ?lists_are_equal([{<<"/+/1">>, self()},{<<"/A/+">>, OtherPid}], mqtt_sub_repo:get_matching_subs(<<"/A/1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Pid = self(),
                            mqtt_sub_repo:claim_filter(<<"/+/1">>, Pid),
                            ?assertEqual({ok,Pid},mqtt_sub_repo:get_filter_claim(<<"/+/1">>))
                        end
                    }
                ]
            %%}
        }.

%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Mar 2015 10:09 PM
%%%-------------------------------------------------------------------
-module(mqtt_reg_repo_test).
-author("Kalin").

-compile(export_all).

-include("test_utils.hrl").

setup()->
    Nodes = [node()],
    mnesia:create_schema(Nodes),
    application:start(mnesia),
    mqtt_reg_repo:create_tables(Nodes,1),
    mqtt_reg_repo:wait_for_tables().

teardown()->
    mqtt_reg_repo:delete_tables(),
    application:stop(mnesia).

reg_repo_test_() ->
        {foreach,
            fun() -> setup() end,
            fun(_) -> teardown() end,
            %%{inparallel,
                [
                    {spawn,
                        fun() ->
                            ?assertMatch(undefined, mqtt_reg_repo:get_registration(<<"Client1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            ?assertMatch(ok,mqtt_reg_repo:unregister(self(),<<"Client1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Pid = spawn(fun() -> exit(normal) end),
                            mqtt_reg_repo:register(Pid,<<"Client1">>),
                            ?assertMatch({ok,Pid}, mqtt_reg_repo:get_registration(<<"Client1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Pid = spawn(fun() -> exit(normal) end),
                            mqtt_reg_repo:register(Pid,<<"Client1">>),
                            mqtt_reg_repo:unregister(Pid,<<"Client1">>),
                            ?assertMatch(undefined, mqtt_reg_repo:get_registration(<<"Client1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Pid1 = spawn(fun() -> exit(normal) end),
                            Pid2 = spawn(fun() -> exit(normal) end), %% just a way to generate a Pid
                            mqtt_reg_repo:register(Pid1,<<"Client1">>),
                            ?assertMatch({{dup_detected,Pid1},_},
                                mqtt_reg_repo:register(Pid2,<<"Client1">>)),
                            ?assertMatch({ok,Pid2}, mqtt_reg_repo:get_registration(<<"Client1">>))
                        end
                    },
                    {spawn,
                        fun() ->
                            Pid1 = spawn(fun() -> exit(normal) end),
                            Pid2 = spawn(fun() -> exit(normal) end), %% just a way to generate a Pid
                            mqtt_reg_repo:register(Pid1,<<"Client1">>),
                            mqtt_reg_repo:unregister(Pid1,<<"Client1">>),
                            ?assertMatch({ok,_}, mqtt_reg_repo:register(Pid2,<<"Client1">>)),
                            ?assertMatch({ok,Pid2}, mqtt_reg_repo:get_registration(<<"Client1">>))
                        end
                    }
                ]
            %%}
        }.

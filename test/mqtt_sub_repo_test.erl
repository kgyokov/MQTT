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

-include_lib("eunit/include/eunit.hrl").

setup()->
    io:format("__________________ Test SETUP ________________________"),
    Nodes = [node()],
    mnesia:create_schema(Nodes),
    application:start(mnesia),
    mqtt_sub_repo:create_tables(Nodes,1),
    mqtt_sub_repo:wait_for_tables().

teardown()->
    mqtt_sub_repo:delete_tables().

my_test_() ->
        {setup,
            fun() -> setup() end,
            fun(_) -> teardown() end,
%%             {foreach,
%%                 fun() -> ok end,
%%                 fun mqtt_sub_repo:clear_tables/0,
                [
                    {spawn,
                        fun() ->
                            mqtt_sub_repo:add_sub(<<"Client1">>, 1, <<"/A/1">>),
                            mqtt_sub_repo:add_sub(<<"Client2">>, 2, <<"/A/2">>),
                            ?assertEqual([], mqtt_sub_repo:get_all(<<"/A/+">>))
                        end
                    }
                ]
%%             }
        }.
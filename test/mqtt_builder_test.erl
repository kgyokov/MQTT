%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Dec 2014 10:42 PM
%%%-------------------------------------------------------------------
-module(mqtt_builder_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").

-define(TEST_MODULE, mqtt_builder).

-compile(export_all).


build_variable_length_0_test() ->
  Result = mqtt_builder:build_var_length(0),
  ?assertEqual(Result, <<0:8>>).

build_variable_length_127_test() ->
  Result = mqtt_builder:build_var_length(127),
  ?assertEqual(Result, <<127:8>>).

build_variable_length_128_byte_test() ->
  Result = mqtt_builder:build_var_length(128),
  ?assertEqual(Result, <<128:8,1:8>>).

build_variable_length_2_byte_test() ->
  Result = mqtt_builder:build_var_length(321),
  ?assertEqual(Result, <<193:8,2>>).

build_variable_length_max_test() ->
  Result = mqtt_builder:build_var_length(268435455),
  ?assertEqual(Result, <<16#FFFFFF7F:32>>).

build_variable_length_overflow_test() ->
  ?assertException(throw, variable_length_too_large, mqtt_builder:build_var_length(268435456)).

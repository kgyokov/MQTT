%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 06. Dec 2014 2:14 PM
%%%-------------------------------------------------------------------
-module(mqtt_parser_test).
-author("Kalin").

-include_lib("eunit/include/eunit.hrl").
-include("mqtt_const.hrl").


-compile([exportall]).

%% -export([parse_string_test/0, parse_string_chunked_test/0, parse_string_chunked_test/0,
%%           parse_string_chunked2_test/0, parse_string_chunked3_test/0,all_tests/0
%% ]).


parse_string_test()->
  String = <<9:16,"123456789">>,
  ReadFun = fun() -> {ok, String } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, <<"123456789">>, <<>>}, mqtt_parser:parse_string(State)).

parse_string_chunked_test()->
  ParseProcess = initialize_parse_process(<<>>, fun mqtt_parser:parse_string/1),

    %% Send string in two separate chunks
  push_fragment(ParseProcess,<<9:16, "12345">>),
  push_fragment(ParseProcess,<<"6789">>),

  ?assertEqual({ok, <<"123456789">>, <<>>},receive_result(ParseProcess))
.

parse_string_chunked2_test()->
  ParseProcess = initialize_parse_process(<<>>,fun mqtt_parser:parse_string/1),

  %% Send string in two separate chunks
  push_fragment(ParseProcess,<<0:8>>),
  push_fragment(ParseProcess,<<9:8>>),
  push_fragment(ParseProcess,<<"12345">>),
  push_fragment(ParseProcess,<<"6789">>),

  ?assertEqual({ok, <<"123456789">>, <<>>} , receive_result(ParseProcess))
.

parse_string_chunked3_test()->
  ParseProcess = initialize_parse_process(<<>>,fun mqtt_parser:parse_string/1),

  %% Send string in two separate chunks
  push_fragment(ParseProcess,<<0:8>>),
  push_fragment(ParseProcess,<<9:8>>),
  push_fragment(ParseProcess,<<"12345">>),
  push_fragment(ParseProcess,<<"6789abc">>),

  ?assertEqual({ok, <<"123456789">>, <<"abc">>}, receive_result(ParseProcess))
.

parse_variable_length_1_byte_test()->
  Length = <<1:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 1, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_1_byte_2_test()->
  Length = <<127:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 127, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_2_bytes_test()->
  Length = <<193:8,2:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 321, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_3_bytes_test()->
  Length = <<193:8,2:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 321, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_4_bytes_test()->
  Length = <<255:8,255:8,255:8,127:8>>,
  ReadFun = fun() -> {ok, Length } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  ?assertEqual({ok, 268435455, <<>>}, mqtt_parser:parse_variable_length(State)).

parse_variable_length_chunked_test()->
  ParseProcess = initialize_parse_process(<<>>, fun mqtt_parser:parse_variable_length/1),
  push_fragment(ParseProcess,<<255:8,255:8>>),
  push_fragment(ParseProcess,<<255:8,127:8>>),

  ?assertEqual({ok, 268435455, <<>>}, receive_result(ParseProcess)).

%%
%%
%% Test Utilities
%%
%%

initialize_parse_process(StartBuffer, Fun)->
  ReadFun = fun() -> receive Fragment -> {ok, Fragment } after 1000 -> {error, timeout} end end,
  State = #parse_state { buffer = StartBuffer, readfun = ReadFun, max_buffer_size = 1000000 },
  Self = self(),
  spawn(fun() -> Self! { self(), Fun(State)} end).

push_fragment(ParseProcess,NewFragment)->
  ParseProcess! NewFragment.


receive_result(ParseProcess)->
  receive
    {ParseProcess, Response} ->
      Response
  after 1000 ->
    throw(unresponsive)
  end.


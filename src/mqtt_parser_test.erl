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

%% API
-export([parse_string_test/0, parse_string_test2/0, parse_string_test3/0, all_tests/0]).

-record(parse_state, {
  readfun,
  buffer,
  max_buffer_size
}).

all_tests()->
  [
  parse_string_test(),
  parse_string_test2(),
  parse_string_test3(),
  parse_string_test4()
  ]
  .

parse_string_test()->
  String = <<9:16,"123456789">>,
  ReadFun = fun() -> {ok, String } end,
  State = #parse_state { buffer = <<>>, readfun = ReadFun, max_buffer_size = 1000000 },
  mqtt_parser:parse_string(State).

parse_string_test2()->
  ParseProcess = initialize_parse_process(<<>>),

    %% Send string in two separate chunks
  push_fragment(ParseProcess,<<9:16, "12345">>),
  push_fragment(ParseProcess,<<"6789">>),

  receive_result(ParseProcess)
.

parse_string_test3()->
  ParseProcess = initialize_parse_process(<<>>),

  %% Send string in two separate chunks
  push_fragment(ParseProcess,<<0:8>>),
  push_fragment(ParseProcess,<<9:8>>),
  push_fragment(ParseProcess,<<"12345">>),
  push_fragment(ParseProcess,<<"6789">>),

  receive_result(ParseProcess)
.

parse_string_test4()->
  ParseProcess = initialize_parse_process(<<>>),

  %% Send string in two separate chunks
  push_fragment(ParseProcess,<<0:8>>),
  push_fragment(ParseProcess,<<9:8>>),
  push_fragment(ParseProcess,<<"12345">>),
  push_fragment(ParseProcess,<<"6789abc">>),

  receive_result(ParseProcess)
.

initialize_parse_process(StartBuffer)->
  ReadFun = fun() -> receive Fragment -> {ok, Fragment } after 1000 -> {error, timeout} end end,
  State = #parse_state { buffer = StartBuffer, readfun = ReadFun, max_buffer_size = 1000000 },
  Self = self(),
  spawn(fun() -> Self! { self(), mqtt_parser:parse_string(State)} end).

push_fragment(ParseProcess,NewFragment)->
  ParseProcess! NewFragment.


receive_result(ParseProcess)->
  receive
    {ParseProcess, Response} ->
      Response
  after 1000 ->
    throw(unresponsive)
  end.


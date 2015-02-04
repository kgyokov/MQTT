%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jan 2015 1:02 AM
%%%-------------------------------------------------------------------
-module(mqtt_topic).
-author("Kalin").

%% API
-export([]).


expand_filter(<<"/":>>)->
0
.

expand_filter()

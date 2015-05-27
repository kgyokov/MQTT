%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. May 2015 12:43 AM
%%%-------------------------------------------------------------------
-author("Kalin").


-record(session_out,{
    client_id                 ::binary(),            %% The id of the client
    packet_seq                ::non_neg_integer(),   %% The latest packet id (incremented by 1 for every packet)
    qos1 = dict:new()         ,
    qos2 = dict:new()         ,
    qos2_rec = gb_sets:new()  ,
    refs = gb_sets:new()      ,
    subs = orddict:new()
}).

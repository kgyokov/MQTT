%%%-------------------------------------------------------------------
%%% @author Kalin
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Mar 2017 4:33 PM
%%%-------------------------------------------------------------------
-module(mqtt_filters_proc).
-author("Kalin").

-behaviour(gen_server).

%% API
-export([start_link/0, claim_filter/2, unclaim_filter/2, get_filter_claim/1, get_matching_filters/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {tree :: mqtt_filter_tree:filter_tree() }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).


claim_filter(Filter,ByPid) ->
    gen_server:call({global,?SERVER},{claim_filter,Filter,ByPid}).

unclaim_filter(Filter,ByPid) ->
    gen_server:call({global,?SERVER},{unclaim_filter,Filter,ByPid}).

get_filter_claim(Filter) ->
    gen_server:call({global,?SERVER},{get_filter_claim,Filter}).

get_matching_filters(Topic) ->
    gen_server:call({global,?SERVER},{get_matching_claim,Topic}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    %%@todo: Move loading a separate process
    {ok, #state{tree = load_filters()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({claim_filter,Filter,ByPid}, _From, #state{tree = Tree}) ->
    Tree1 = mqtt_filter_tree:update_raw_filter(Filter,Tree,fun add_filter_and_pid/2),
    mqtt_sub_repo:claim_filter(Filter,ByPid),
    {reply, ok, #state{tree = Tree1}};

handle_call({unclaim_filter,Filter,ByPid}, _From, #state{tree = Tree}) ->
    Tree1 = mqtt_filter_tree:update_raw_filter(Filter,Tree,fun remove_pid/2),
    mqtt_sub_repo:claim_filter(Filter,ByPid),
    {reply, ok, #state{tree = Tree1}};

handle_call({get_filter_claim,Filter,ByPid}, _From, S = #state{tree = Tree}) ->
    Tree1 = mqtt_filter_tree:update_raw_filter(Filter,Tree,fun remove_pid/2),
    mqtt_sub_repo:claim_filter(Filter,ByPid),
    {reply, ok, S};

handle_call({get_matching_claim,Topic}, _From, State) ->
    {reply, ok, State};

handle_call(_Request, _From, S) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

default_set_if_nil(nil) -> gb_sets:empty();
default_set_if_nil(Set) -> Set.

add_filter_and_pid({Filter,Pid},Tree) ->
    UpdateFun = fun(Set) ->
                    Set1 = default_set_if_nil(Set),
                    gb_sets:add_element(Pid,Set1)
                end,
    mqtt_filter_tree:update_raw_filter(Filter,Tree,UpdateFun).

remove_pid(Filter,Tree) ->
    UpdateFun = fun(Set) ->
                    Set1 = default_set_if_nil(Set),
                    gb_sets:del_element(Filter,Set1)
                end,
    mqtt_filter_tree:update_raw_filter(Filter,Tree,UpdateFun).

get_filter_claim(Filter,Tree) -> mqtt_filter_tree:get_vals_for_filter(Filter,Tree).
get_matching_claims(Topic,Tree) ->
    Iter = mqtt_filter_tree:get_iterator_for_matching_filters(Topic,Tree),
    iter:to_list(Iter).

load_filters() -> mqtt_sub_repo:foldl(fun add_filter_and_pid/2,mqtt_filter_tree:new()).

-module(cast_node_mgr).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {node_list = []}).

-include_lib("eunit/include/eunit.hrl").

-export([
         add_node/1,
         check_node/1
        ]).

%%%===================================================================
%%% API functions
%%%===================================================================

-define(ETS_NAME,         ets_check_node_mcast).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_node( Node ) ->
    ets:insert(?ETS_NAME, { Node, true}),
    ok.

-spec check_node( Node::atom() ) -> {ok, boolean()}.
check_node( Node ) ->
    case ets:lookup( ?ETS_NAME, Node) of
        [] ->
            {ok, false};
        [_Have] ->
            {ok, true}
    end.

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
init([]) ->
    ets:new(?ETS_NAME, [named_table, public, set]),
    {ok, #state{ node_list = [] }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({check_node, Node}, _From, State) ->
    {reply, {ok, lists:member(Node, State#state.node_list) }, State};

handle_call({add_node, Node}, _From, State) ->
    OldNodeList = State#state.node_list,
    {reply, ok, State#state{node_list = [Node | OldNodeList]}};

handle_call(_Request, _From, State) ->
    lager:error("request can't match ~p ~p ~n",[_Request, State]),
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
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
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

svc_test() ->
    start_link(),
    ?assertEqual(ok, add_node( 'hello@localhost' )),
    ?assertEqual({ok, true}, check_node( 'hello@localhost') ),
    ?assertEqual({ok, false}, check_node( 'hello@localhost1') ).
    
       




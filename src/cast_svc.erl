

-module(cast_svc).

-behaviour(gen_server).

%% API functions
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-include_lib("eunit/include/eunit.hrl").
-define(TIMER_INTERVAL,     5).

-export([
         cast/4,
         size/1,
         get_env/2,
         do_get_server_name/1,
         test_perform/0
        ]).

-define(MAX_QUEUE_SIZE,         500000).
-define(PACKAGE_SIZE,           1000).
-define(FORCE_GC,               12 * 60 * 1000).

-record(state, { 
                node = undefined,
                max_queue_size = ?MAX_QUEUE_SIZE,
                package_size = ?PACKAGE_SIZE,
                temp_queue = [],
                fix_timer = 0,
                last_timer = 0,
                length = 0
               }).

get_env(Key, Default) ->
    case application:get_env(mcast, Key) of
    {ok, Value} ->
            Value;
        _ ->
            Default
    end.

timer_interval( Interval ) ->
	erlang:send_after( Interval, self(), {timer_interval}).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link( Node ) ->
    gen_server:start_link({local, do_get_server_name( Node ) }, ?MODULE, [ Node ], []).

force_gc() ->
    erlang:send_after( get_env( force_gc, ?FORCE_GC), self(), {force_gc}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

do_get_server_name( Node ) ->
    list_to_atom("cast_" ++ atom_to_list(Node) ).

size(Node) ->
    case whereis(do_get_server_name( Node )) of
        undefined ->
            {ok, 0};
        Pid ->
            gen_server:call(Pid, {size})
    end.

% ---------------------------------------------------------------------
% @doc 自己封装好的 cast 函数
% ---------------------------------------------------------------------
-spec cast(Node::atom(), M::atom(), F::atom(), A::list()) -> ok.
cast(Node, M, F, A) ->
    ServerName = do_get_server_name( Node ),
    case cast_node_mgr:check_node( Node ) of
        {ok, true} ->
            gen_server:cast( ServerName, {mcast, M, F, A} );
        _OthersNoThisServer ->
            supervisor:start_child(cast_svc_sup, [ Node ]),
            timer:sleep( 1000 ),
            case whereis(ServerName) of
                undefined ->
                    skip;
                _Pid ->
                    cast_node_mgr:add_node( Node )
            end,
            cast( Node, M, F, A)
    end. 

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
init([ Node ]) ->

    LoopTimer = get_env( loop_interval, ?TIMER_INTERVAL ),

    force_gc(),
    timer_interval( LoopTimer ),

    MaxQueueSize = get_env( mcast_max_queue_size, ?MAX_QUEUE_SIZE ),
    PackageSize = get_env( package_size, ?PACKAGE_SIZE ),

    my_queue:new( util:ceil( MaxQueueSize / PackageSize ) ),

    {ok, #state{ node = Node , 
                 temp_queue = [],
                 length = 0, 
                 package_size = PackageSize,
                 fix_timer = LoopTimer,
                 last_timer = util:longunixtime(),
                 max_queue_size = MaxQueueSize}}.

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
handle_call({size}, _From, State) ->
    {reply, {ok, State#state.length}, State};
handle_call(_Request, _From, State) ->
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
handle_cast({mcast, M, F, A}, State) ->
    case State#state.length >= ?MAX_QUEUE_SIZE of
        true ->
            {noreply, State};
        _NotBigger ->
            NewLen = State#state.length + 1,
            case length(State#state.temp_queue) =:= ?PACKAGE_SIZE - 1 of
            true ->
                Block = [{M, F, A} | State#state.temp_queue ],
                my_queue:in_block( Block ),
                {noreply, State#state{ length = NewLen, temp_queue = []}};
            _NotFull ->
                NewQueue = [ {M, F, A} | State#state.temp_queue ],
                {noreply, State#state{ length = NewLen, temp_queue = NewQueue }}
            end
    end;
handle_cast(_Msg, State) ->
    util:print("uncaut msg ~p ~n",[_Msg]),
    {noreply, State}.


do_send_block_list(BlockList, Node) ->
   Fun = fun() ->
                Bin = zlib:zip(erlang:term_to_binary( BlockList )),
                rpc:cast( Node, cast_recv, cast, [Bin] )
   end,
   spawn( Fun ).

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
handle_info({force_gc}, State) ->
    force_gc(),
    spawn(fun() -> erlang:garbage_collect( self() ) end),
    {noreply, State};
handle_info({timer_interval}, State) ->
    NowTime = util:longunixtime(),
    NextTimer = min(State#state.fix_timer,  max(0, State#state.fix_timer * 2 - NowTime + State#state.last_timer)),
    timer_interval( NextTimer ),
    case my_queue:empty() of
        true ->
            case State#state.temp_queue of
                [] ->
                    {noreply, State#state{ last_timer = NowTime }, hibernate};
                _NotEmpty ->
                    do_send_block_list( State#state.temp_queue, State#state.node ),
                    {noreply, State#state{temp_queue = [], length = 0, last_timer = NowTime }}
            end;
        _NotEmpty ->
            BlockList = my_queue:out() ++ my_queue:out() ++ my_queue:out() ++ my_queue:out() ++ State#state.temp_queue,
            do_send_block_list( BlockList, State#state.node ),
            % util:print("SendLen ~p ~n",[length(BlockList)]),
            NewLength = max(0,  State#state.length - length(BlockList) ),
            {noreply, State#state{ length = NewLength, last_timer = NowTime, temp_queue = []}}
    end;
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

get_server_name_test() ->
    Node = 'h@localhost',
    ?assertEqual( 'cast_h@localhost' , do_get_server_name( Node )),
    ok.

do_random_uid(Range) ->
    integer_to_binary(random:uniform(Range)).

test_perform() ->
    UidRange = 100000000,
    Fun = fun(X, _AccList) ->
        Bin = integer_to_binary(X),
        A = [do_random_uid(UidRange) , do_random_uid(UidRange), <<"sdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsss    dfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfa    dsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsf    dsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdss    sdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdsssdfadsfdss", Bin/binary>>, 2],
        erlang:apply( log_server_core, msg_info, A )
    end,
    FunTimes = fun(_X, _Acc) ->
        % lager:info("Times:~p ~n",[X]),
        lists:foldl(Fun, 0, lists:seq(1, 14000) ),
        timer:sleep(2000)
    end,
    spawn(fun() -> lists:foldl(FunTimes, 0, lists:seq(1, 2000)) end),
    ok.


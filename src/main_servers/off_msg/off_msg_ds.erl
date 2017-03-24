-module(off_msg_ds).

-behaviour(gen_server).

%% API functions
-export([start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([delete/2, get_server_name/1]).

-record(state, {make_sql_fun = undefind}).

%%%===================================================================
%%% API functions
%%%===================================================================


-define( QUEUE_MAX_LEN,     500 * 10000 ).
-define( BLOCK_SIZE,        1000).
-define( TIMER_INTERVAL,    1000).
-define( FORCE_GC_TIME,     12 * 60 * 1000).

-include("off_msg.hrl").

get_server_name( tmp ) ->
    off_msg_tmp_ds;
get_server_name( index ) ->
    off_msg_index_ds.

start_link( ServerName, MakeSqlFun ) ->
    gen_server:start_link({ local, ServerName }, ?MODULE, [ MakeSqlFun ], []).

init([ MakeSqlFun ]) ->
    my_queue:new( util:ceil( ?QUEUE_MAX_LEN / ?BLOCK_SIZE ) ),
    force_gc(),
    timer_interval(),
    {ok, #state{ make_sql_fun = MakeSqlFun }}.

delete(ServerName, ParamList ) ->
    gen_server:cast( ServerName, {delete_tmp, ParamList }).

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({delete_tmp, MsgId}, State) ->
    my_queue:in(MsgId, ?BLOCK_SIZE ),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timer_interval}, State ) ->
    Block = my_queue:out(),
    case Block of
        [] ->
            skip;
        _NotEmpty ->
            MakeSqlFun = State#state.make_sql_fun,
            Sql = MakeSqlFun( Block ),
            mysql_poolboy:query( ?OFF_MSG_MYSQL_POOL, Sql )
    end,
    timer_interval( ),
    {noreply, State};
handle_info({force_gc}, State) ->
    force_gc(),
    spawn(fun() -> erlang:garbage_collect( self() ) end),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

% -------------------------------------------------------------------
% @private 启动定时器
% -------------------------------------------------------------------
timer_interval( ) ->
    erlang:send_after( ?TIMER_INTERVAL, self(), {timer_interval}).

force_gc() ->
    erlang:send_after( ?FORCE_GC_TIME, self(), {force_gc}).



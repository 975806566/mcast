-module(off_msg_mgr).

-behaviour(gen_server).

%% API functions
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([
        get_worker_name/2,
        update_worker_status/2,
        get_worker_status/1,
        get_mgr_name/1,
        insert/2,
        test/1
        ]).

-include("off_msg.hrl").

-record(state, { worker_map = #{} , temp_queue = []}).

-define( QUEUE_MAX_LEN,     500 * 10000 ).
-define( BLOCK_SIZE,        1000).
-define( TIMER_INTERVAL,    1000).
-define( FORCE_GC_TIME,     12 * 60 * 1000).

-define( LENGTH_KEY,        '$length_key').
-define( DEFAULT_TIMEOUT,   5000 ).
get_mgr_name( WorkerNameHeader ) ->
    list_to_atom( WorkerNameHeader ++ "_"  ++ util:a2l(?MODULE)  ).
    

start_link(WorkerNameHeader, SqlHead, SqlTail) ->
    gen_server:start_link({local, get_mgr_name( WorkerNameHeader )}, ?MODULE, [WorkerNameHeader, SqlHead, SqlTail ], []).


insert(MgrName, ValueList) ->
    gen_server:cast( MgrName, { insert, ValueList} ).

% --------------------------------------------------------------------
% @doc 更新 worker 进程的状态
% --------------------------------------------------------------------
update_worker_status( WorkerName , Status) ->
    ets:insert( ?OFF_MSG_MGR_ETS, {WorkerName, Status} ).

get_worker_status( WorkerName ) ->
    case ets:lookup( ?OFF_MSG_MGR_ETS, WorkerName ) of
        [] ->
            ?OFF_MSG_STATUS_BUSY;
        [{_WorkName, Res}] ->
            Res
    end.

% --------------------------------------------------------------------
% @doc 获取工作进程的名字
% --------------------------------------------------------------------
get_worker_name(WorkerNameHeader, I ) ->
    list_to_atom(WorkerNameHeader ++ "_off_msg_worker_" ++ util:a2l(I)).

% --------------------------------------------------------------------
% @doc 启动工作进程
% --------------------------------------------------------------------
do_start_worker( WorkerName ) ->
    SqlHead = get(sql_head),
    SqlTail = get(sql_tail),
    WorkerNameHeader = get( worker_name_header ),
    ChildSpec =
    case util:get_env( mcast, off_server_db_type, mysql ) of
        {ok, mysql} ->
            { WorkerName, { off_msg_mysql_worker, start_link, [ WorkerName, SqlHead, SqlTail, WorkerNameHeader ]},
                                permanent, brutal_kill, worker, [ off_msg_mysql_worker ]};
        {ok, NotMysql} ->
            lager:error("not support this db ~p~n",[ NotMysql ]),
            {WorkerName, { off_msg_mysql_worker  , start_link, [ WorkerName, SqlHead, SqlTail, WorkerNameHeader ]},
                                permanent, brutal_kill, worker, [ off_msg_mysql_worker ]}
    end,
    supervisor:start_child( off_msg_sup, ChildSpec ).

% --------------------------------------------------------------------------
% @doc 获取最大的队列长度
% --------------------------------------------------------------------------
do_get_max_queue_len() ->
    {ok, Len} = util:get_env(mcast, off_server_max_queue_len, ?QUEUE_MAX_LEN ),
    Len.

% ---------------------------------------------------------------------------
% @doc 获取最大的 Block size
% ---------------------------------------------------------------------------
do_get_block_size() ->
    {ok, BlockSize} = util:get_env(mcast, off_server_block_size, ?BLOCK_SIZE ),
    BlockSize.
    

init([ WorkerNameHeader, SqlHead, SqlTail]) ->

    util:print("-------------------------------  init off_msg_mgr_tmp ----------------------- ~n",[]),
    % ---------------------------------------------------- %
    %-% 写入这个管理进程的 sql 语句
    % ---------------------------------------------------- %
    put( sql_head,              SqlHead ),
    put( sql_tail,              SqlTail ),
    put( worker_name_header,    WorkerNameHeader), 

    % ----------------------------------------------------- %
    %-% 管理进程记录他自己所监管的进程列表 
    % ---------------------------------------------------- %
    Schedulers = erlang:system_info(schedulers),
    Fun = fun(I, AccMaps) ->
        WorkerName = get_worker_name( WorkerNameHeader, I ),
        update_worker_status( WorkerName, ?OFF_MSG_STATUS_BUSY ),
        maps:put( WorkerName, false, AccMaps )
    end,
    NewMaps = lists:foldl( Fun, #{}, lists:seq(1, Schedulers) ),

    % ----------------------------------------------------- %
    %-% 新建缓冲队列
    % ----------------------------------------------------- %
    MaxQueueSize = do_get_max_queue_len(),
    BlockSize = do_get_block_size(),
    my_queue:new( util:ceil( MaxQueueSize / BlockSize ) ),
    
    util:print("--------------------- maxQueueSize:~p BlockSize:~p ~n",[MaxQueueSize, BlockSize]),
    % ----------------------------------------------------- %
    %-% 启动他自己所监管的写数据库的工作进程
    % ----------------------------------------------------- %
    force_gc(),
    self() ! { start, all, workers },
    timer_interval( ?TIMER_INTERVAL ),
    
    put( max_queue_size, MaxQueueSize ),
    put( block_size,     BlockSize ),
    put( ?LENGTH_KEY,         0 ),
    {ok, #state{ worker_map = NewMaps  ,temp_queue = [] }}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({ insert, ValueList}, State) ->
    case get( ?LENGTH_KEY  ) >= get(max_queue_size) of
        true ->
            {noreply, State};
        _NotBigger ->
            add_len( 1 ),
            case length(State#state.temp_queue) =:= get( block_size )  - 1 of
            true ->
                Block = [ ValueList | State#state.temp_queue ],
                my_queue:in_block( Block ),
                {noreply, State#state{ temp_queue = []}};
            _NotFull ->
                NewQueue = [ ValueList  | State#state.temp_queue ],
                {noreply, State#state{ temp_queue = NewQueue }}
            end
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timer_interval}, State) ->
    timer_interval( ?TIMER_INTERVAL ),
    WorkerList = maps:to_list( State#state.worker_map ),
    
    [{LastName, _LastStatus0} | ExList ] = WorkerList,
    LastStatus = get_worker_status( LastName ),

%    util:print("tempQueue:~p ~n",[State#state.temp_queue]),

    %-% 留出一个来判断是否需要发送tempQueue，先发送剩余的
    % -------------------------------------------------------------------------------
    FunSendEx = 
        fun( {WorkerName, _Status}, _Acc ) ->
            Status = get_worker_status( WorkerName ),
            case Status of
                ?OFF_MSG_STATUS_IDLE ->
                    case my_queue:empty() of
                        true ->
                            skip;
                        _HaveData ->
                            Block = my_queue:out(),
                            sub_len( length(Block) ),
                            off_msg_mysql_worker:insert_data( WorkerName, Block )
                    end;
                _NotIdle ->
                    util:print(" -- busy --  ~p ~n",[ WorkerName ]),
                    skip
            end
        end,
    lists:foldl(FunSendEx, 0, ExList),
    % --------------------------------------------------------------------------------
    
    case LastStatus of
        ?OFF_MSG_STATUS_IDLE ->
            case my_queue:empty() of
                true ->
                    case State#state.temp_queue of
                        [] ->
                            {noreply, State};
                        _TempQueueNotEmpty ->
                            off_msg_mysql_worker:insert_data( LastName, State#state.temp_queue ),
                            set_len( 0 ),
                            {noreply, State#state{ temp_queue = [] }}
                    end;
                _NotEmpty ->
                    LastBlock = my_queue:out(),
                    sub_len( length( LastBlock ) ),
                    off_msg_mysql_worker:insert_data( LastName, LastBlock ),
                    {noreply, State}
            end;
        _Busy ->
            util:print(" -- busy ~p ~n",[ LastName ]),
            {noreply, State}
    end;
handle_info({force_gc}, State) ->
    force_gc(),
    spawn(fun() -> erlang:garbage_collect( self() ) end),
    {noreply, State};
handle_info( {start, all, workers} , State) ->
    WorkerList = maps:to_list(State#state.worker_map),
    Fun = fun( { WorkerName, _Status }, _Acc) ->
        { ok, _Child } = do_start_worker( WorkerName )
    end,
    lists:foldl(Fun, 0, WorkerList),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

force_gc() ->
    erlang:send_after( ?FORCE_GC_TIME, self(), {force_gc}).

% -------------------------------------------------------------------
% @private 启动定时器
% -------------------------------------------------------------------
timer_interval( Interval ) ->
    erlang:send_after( Interval, self(), {timer_interval}).


set_len( Len ) ->
    put( ?LENGTH_KEY, Len).

add_len( Add ) ->
    set_len( get( ?LENGTH_KEY ) + Add ).

sub_len( Sub ) ->
    set_len( max(0, get( ?LENGTH_KEY ) - Sub)).

do_random_uid(Range) ->
    util:a2b(random:uniform(Range)).

test(N) ->
    test_1( N ).

test_insert( I ) ->
    IdPre = I * 100000000,
    UidRange = 10000000,
    Fun = fun( X, _Acc ) ->
        MsgId = IdPre + X,
        Bin = util:a2b( X ),
        insert( get_mgr_name(?OFF_MSG_TMP), [ MsgId, do_random_uid(UidRange), do_random_uid(UidRange),  <<"12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678912345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678900123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", Bin/binary>>, 1, util:unixtime() ]),
        insert( get_mgr_name(?OFF_MSG_OFF), [ do_random_uid( UidRange ), MsgId, util:unixtime() ]) 
    end,
    lists:foldl( Fun, 0, lists:seq(1, 2000) ).

test_1(N) ->
    Fun = fun( I, _Acc ) ->
        test_insert( I ),
        timer:sleep( 1000 )
    end,
    spawn ( fun() -> lists:foldl( Fun, 0, lists:seq(1, N)) end),
    ok.






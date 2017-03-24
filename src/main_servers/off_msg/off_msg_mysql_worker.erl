-module(off_msg_mysql_worker).

-behaviour(gen_server).

%% API functions
-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
                name = 1,
                status = false,
                msg_queue = []
                }).

-export([
         insert_data/2
        ]).

%%%===================================================================
%%% API functions
%%%===================================================================

-include("off_msg.hrl").

-define( EACH_PACKAGE_SIZE, 40 ).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link( WorkerName, SqlHead, SqlTail, WorkerNameHeader ) ->
    gen_server:start_link({local, WorkerName}, ?MODULE, [WorkerName, SqlHead, SqlTail, WorkerNameHeader ], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

update_writer_status( ?OFF_MSG_TMP, BlockList ) ->
    spawn( 
            fun() ->
                    [ off_msg_tmp_cache:update_save_flag( MsgId )  || [MsgId | _NotCare] <- BlockList  ]
            end);
update_writer_status( ?OFF_MSG_OFF,  BlockList ) ->
    spawn(
            fun() ->
                    [ off_msg_index_cache:save_ok_event( MsgId, UserId )  || [ UserId, MsgId | _NotCare] <- BlockList  ]
            end
    ).


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
init([ WorkerName, SqlHead, SqlTail, WorkerNameHeader ]) ->

    put(sql_head, SqlHead),
    put(sql_tail, SqlTail),
    put(worker_name_herder, WorkerNameHeader ),
    off_msg_mgr:update_worker_status( WorkerName, ?OFF_MSG_STATUS_IDLE ),

    {ok, #state{
                name = WorkerName,
                status = ?OFF_MSG_STATUS_IDLE,
                msg_queue = []
    }}.

% --------------------------------------------------------------------
% @doc 把输入写入到写进程
% --------------------------------------------------------------------
insert_data(_WorkerName, []) ->
    ok;
insert_data( WorkerName, MsgList) ->
    gen_server:call( WorkerName, {insert_data, MsgList} ).


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
handle_call({insert_data, MsgList}, _From, State) ->
    self() ! {insert, off_msg, mysql},
    off_msg_mgr:update_worker_status(State#state.name, ?OFF_MSG_STATUS_BUSY),
    {reply, {ok}, State#state{msg_queue = MsgList}};
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
handle_cast(_Msg, State) ->
    {noreply, State}.


make_sql(SqlRefHead, SqlRef , Args) ->
    Len = length(Args),
    case get({prepare, Len}) of
        undefined ->
            [ _H | SqlRefs ] = lists:flatten(lists:duplicate(Len, SqlRef)),
            Sql0 = SqlRefHead ++ SqlRefs,
            put( {prepare, Len}, Sql0),
            Sql0;
        Sql ->
            Sql
    end.

handle_info({ insert, off_msg, mysql }, State) ->
    try
        Queue = State#state.msg_queue,
        MsgLists = util:splite_lists( Queue, util:ceil(length(Queue) / ?EACH_PACKAGE_SIZE) ),
        
        Fun =
        fun(E, Pid) ->
            NewSql = make_sql(get(sql_head), get(sql_tail), E),
            try
               ok = mysql:query(Pid, NewSql, lists:concat( E ) ),
               update_writer_status( get( worker_name_herder ), E)
            catch
                _IM:_IR ->
                    lager:error("M:~p R:~p ~n~p ~n",[_IM, _IR, erlang:get_stacktrace()])
            end,
            Pid
        end,
        mysql_poolboy:with( ?OFF_MSG_MYSQL_POOL, fun (Pid) ->
            lists:foldl( Fun, Pid, MsgLists)
        end),
        off_msg_mgr:update_worker_status(State#state.name, ?OFF_MSG_STATUS_IDLE),
       { noreply, State#state{msg_queue = []}}
    catch
       M: R ->
         lager:error("M:~p R:~p ~n~p ~n",[M, R, erlang:get_stacktrace()]),
         off_msg_mgr:update_worker_status(State#state.name, ?OFF_MSG_STATUS_IDLE),
         { noreply, State}
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

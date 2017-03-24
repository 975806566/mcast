-module(off_msg_tmp_cache).

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

-export([
        pool/0,
        new_tmp_group_msg/6,
        new_tmp_person_msg/8,
        update_save_flag/1,
        delete_tmp/1,
        ack_event/1,
        get_tmp_from_db/1,
        get_tmp/1
        ]).

-record(state, { id }).

-define( TMP_CACHE_POOL,        tmp_cache_pool ).
-define( FORCE_GC_TIME,         12 * 60 * 1000).

-include("off_msg.hrl").

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
start_link( Id ) ->
    gen_server:start_link(?MODULE, [ Id ], []).

force_gc() ->
    erlang:send_after( ?FORCE_GC_TIME, self(), {force_gc}).

get_tmp_from_db( MsgId ) ->
    Sql = "select from_id,topic,content,type,qos,time from msg_tmp where msg_id = ?;",
    case mysql_poolboy:query( ?OFF_MSG_MYSQL_POOL, Sql, [ MsgId ] ) of
        {ok, _Fields, ResList} ->
            ResList;
        _OthersSkip ->
            []
    end.

get_tmp( MsgId ) ->
    case mnesia:dirty_read( off_msg_tmp, MsgId ) of
        [] ->
            case get_tmp_from_db( MsgId ) of
                [[ FromId, Topic, Content, Type, Qos, Time]] ->
                    CmPid = gproc_pool:pick_worker( ?TMP_CACHE_POOL, MsgId ),
                    gen_server:cast( CmPid, { load_tmp_msg, MsgId, FromId, Topic, Content, Qos, Type, Time}),
                    #off_msg_tmp{msg_id = MsgId,
                                 from_id = FromId,
                                 topic = Topic,
                                 type = Type,
                                 qos = Qos,
                                 content = Content,
                                 time = Time,
                                 last_update_time = util:longunixtime()  
                    };
                _Error ->
                    []
            end;
        [ OffMsgTmp ] ->
            OffMsgTmp
    end.

ack_event( MsgId ) ->
    CmPid = gproc_pool:pick_worker( ?TMP_CACHE_POOL, MsgId ),
    gen_server:cast( CmPid, { ack_event, MsgId }).  

% --------------------------------------------------------------------
% @doc 删除已经过期的缓存
% --------------------------------------------------------------------
delete_tmp( MsgId ) ->
    CmPid = gproc_pool:pick_worker( ?TMP_CACHE_POOL, MsgId ),
    gen_server:cast( CmPid, { delete_tmp, MsgId }).
    
new_tmp_group_msg(MsgId, From, Topic, Content, Qos, NowTime ) ->
    CmPid = gproc_pool:pick_worker( ?TMP_CACHE_POOL, MsgId ),
    gen_server:cast( CmPid, { new_tmp_msg,MsgId, From, Topic, Content, Qos, <<"g">>, NowTime }).

new_tmp_person_msg(ToId, MsgId, From, Topic, Content, Qos, ApnsInfo, NowTime ) ->
    CmPid = gproc_pool:pick_worker( ?TMP_CACHE_POOL, MsgId ),
    off_msg_index_cache:new_index_msg( ToId, MsgId, Qos, <<"p">>, ApnsInfo, util:longunixtime() ),
    gen_server:cast( CmPid, { new_tmp_msg,MsgId, From, Topic, Content, Qos, <<"p">>, NowTime }).

% ---------------------------------------------------------------------
% @doc 更新服务是否已经写入缓存的状态
% ---------------------------------------------------------------------
update_save_flag( MsgId ) ->
    CmPid = gproc_pool:pick_worker( ?TMP_CACHE_POOL, MsgId ),
    gen_server:cast( CmPid, {save_flag, MsgId}).

pool() -> ?TMP_CACHE_POOL.

init([ Id ]) ->
    gproc_pool:connect_worker( ?TMP_CACHE_POOL, {?MODULE, Id}),
    force_gc(),
    {ok, #state{id = Id}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

% ----------------------------------------------------------------------------------
% @doc 一个进程写，多个进程读，避免死锁
% ----------------------------------------------------------------------------------
handle_cast({ack_event, MsgId}, State) ->
    mnesia:dirty_delete( off_msg_tmp, MsgId ),
    off_msg_ds:delete(off_msg_ds:get_server_name(tmp), [ MsgId ] ),
    {noreply, State};
handle_cast({delete_tmp, MsgId}, State) ->
    mnesia:dirty_delete( off_msg_tmp, MsgId ),
    {noreply, State};
handle_cast({save_flag, MsgId}, State) ->
    case mnesia:dirty_read( off_msg_tmp , MsgId ) of
        [] ->
            skip;
        [ OffMsgRc ] ->
            mnesia:dirty_write( off_msg_tmp, OffMsgRc#off_msg_tmp{save_flag = true} )
    end,
    {noreply, State};
handle_cast( { load_tmp_msg, MsgId, From, Topic, Content, Qos, Type, Time}, State ) ->
        NowTime = util:longunixtime(),
        mnesia:dirty_write(off_msg_tmp, #off_msg_tmp{
                                                    msg_id = MsgId, 
                                                    from_id = From,
                                                    topic = Topic,
                                                    content = Content,
                                                    type = Type,
                                                    qos = Qos,
                                                    save_flag = true,
                                                    time = Time,
                                                    last_update_time = NowTime
                                                }),
    {noreply, State};
handle_cast( { new_tmp_msg,NewMsgId, From, Topic, Content, Qos, Type, NowTime }, State) ->
    try
        mnesia:dirty_write(off_msg_tmp, #off_msg_tmp{
                                                    msg_id = NewMsgId, 
                                                    from_id = From,
                                                    topic = Topic,
                                                    content = Content,
                                                    type = Type,
                                                    qos = Qos,
                                                    save_flag = false,
                                                    time = NowTime,
                                                    last_update_time = NowTime
                                                }),
        off_msg_mgr:insert( off_msg_mgr:get_mgr_name(?OFF_MSG_TMP), [ NewMsgId, From, Topic, Content, Type, Qos, NowTime])
    catch
        M:R ->
            lager:error("M:~p R:~p ~n erlangTrace:~p ~n",[M, R, erlang:get_stacktrace()])
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({force_gc}, State) ->
    force_gc(),
    spawn(fun() -> erlang:garbage_collect( self() ) end),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

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



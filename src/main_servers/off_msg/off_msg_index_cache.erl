-module(off_msg_index_cache).

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

-record(state, { id = 0
               }).

-include_lib("eunit/include/eunit.hrl").

-export([save_ok_event/2]).

-export([
        pool/0,
        new_index_msg/6,
        im_ack/2,
        get_off_msg_event/3,
        timeout_event/1,
        clean_offmsg/1
        ]).

-include("off_msg.hrl").

-define( INDEX_CACHE_POOL,  index_cache_pool ).

-define( OFF_MSG_LIMIT,         1000 ).  % 离线消息发往前端的最大条数
-define( OFF_MSG_LIMIT_HIGH,    1400 ).   % 离线消息高水位

-define( FORCE_GC_TIME,         12 * 60 * 1000).

start_link( Id ) ->
    gen_server:start_link( ?MODULE, [ Id ], []).

do_get_off_msg_limit() ->
    util:get_value_cache( mcast, off_msg_limit, ?OFF_MSG_LIMIT ).

do_get_off_msg_limit_high() ->
    trunc( do_get_off_msg_limit() * 1.4 ).

% --------------------------------------------------------------------
% @doc 离线消息的索引缓冲池
% --------------------------------------------------------------------
pool() ->
    ?INDEX_CACHE_POOL.


force_gc() ->
    erlang:send_after( ?FORCE_GC_TIME, self(), {force_gc}).


% --------------------------------------------------------------------
% 控制用户数据存储到内存中的速度。
% 一秒钟最多置换 10 个用户，由于这个数据在内存中占用比较小。
% 所以多放一会也没关系
% --------------------------------------------------------------------
timeout_event( UserId ) ->
    CmPid = gproc_pool:pick_worker( pool(), UserId ),
    case erlang:process_info( CmPid, message_queue_len  ) of
        {_MessageQL, Len} ->
            case Len >= 1000 of
                true ->
                    skip;
                _Idle ->
                    gen_server:cast( CmPid, { timeout_event, UserId })
            end;
        _CanRec ->
            skip
    end,

    timer:sleep(1000).

clean_offmsg( UserId ) ->
    CmPid = gproc_pool:pick_worker( pool(), UserId ),
    gen_server:cast( CmPid, { clean_offmsg, UserId }).
    

save_ok_event( MsgId, UserId ) ->
    CmPid = gproc_pool:pick_worker( pool(), UserId ),
    gen_server:cast( CmPid, { save_ok_event, MsgId, UserId }).
 

get_off_msg_event(FromNode, UserId, MsgLimit ) ->
    CmPid = gproc_pool:pick_worker( pool(),  UserId  ),
    gen_server:cast( CmPid, { get_off_msg_event,FromNode, UserId, MsgLimit }).
 
new_index_msg( UserId, MsgId,Qos, Type, ApnsInfo, NowTime ) ->
    CmPid = gproc_pool:pick_worker( pool(), UserId ),
    gen_server:cast( CmPid, { new_index_msg, UserId, MsgId, Qos, Type, ApnsInfo, NowTime }).

im_ack( MsgId, UserId ) ->
    CmPid = gproc_pool:pick_worker( pool(), UserId ),
    gen_server:cast( CmPid, { im_ack, MsgId, UserId }).

do_send_apns_msg(_UserId, not_ios ) ->
    skip;
do_send_apns_msg(UserId, {NickName, AppKey, Token, Content0} ) ->
    util:print("apns send ~p ~p ~p ~p ~p ~n",[ UserId, NickName, AppKey, Token, Content0 ]),
    {NickName1, Content1} =
    case NickName of
        {NickNameEx, ContentEx} ->
            {NickNameEx, ContentEx};
        _NotOk ->
            {NickName, Content0}
    end,
            
    Count1 = 
    case mnesia:dirty_read( off_msg_index, UserId ) of
        [] ->
            0;
        [ UserIndex ] ->
            dict:size( UserIndex#off_msg_index.msg_id_list )
    end,
    Count2 = off_msg_index_lib:get_off_msg_index_count( UserId ),
    emqttd_apns:send_apns_message(NickName, AppKey, Token, <<NickName1/binary, ": ", Content1/binary>>, Count1 + Count2 + 1).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([ Id ]) ->
    gproc_pool:connect_worker( ?INDEX_CACHE_POOL, {?MODULE, Id}),
    force_gc(),
    {ok, #state{ id = Id  }}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({clean_offmsg, UserId}, State) ->
    mnesia:dirty_delete( off_msg_index, UserId ),
    off_msg_ds:delete(off_msg_ds:get_server_name( index ), [ UserId ] ),
    {noreply, State};

handle_cast({timeout_event, UserId}, State) ->
    do_timeout_event( UserId ),
    {noreply, State};

handle_cast({save_ok_event, MsgId, UserId}, State) ->
    do_save_ok_event( MsgId, UserId ),
    {noreply, State};

handle_cast({get_off_msg_event, FromNode, UserId, MsgLimit}, State) ->
    do_off_msg_event(FromNode, UserId, MsgLimit ),
    {noreply, State};

handle_cast({ im_ack, MsgId, UserId }, State ) ->
    do_im_ack( MsgId, UserId ),
    {noreply, State};

handle_cast({new_index_msg, UserId, MsgId,Qos, Type, ApnsInfo, NowTime}, State) ->
    do_send_apns_msg( UserId, ApnsInfo ),
    do_new_index_msg( UserId, MsgId,Qos, Type, NowTime),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

% ----------------------------------------------------------------------------------------------------------
% 清空这条消息的缓存
% ----------------------------------------------------------------------------------------------------------
do_delete_cache( UserId, MsgId ) ->
    case mnesia:dirty_read( off_msg_index, UserId ) of
        [] ->
            skip;
        [ OffMsgIndex ] ->
            NewOffMap = dict:erase( MsgId, OffMsgIndex#off_msg_index.msg_id_list ),
            case dict:size( NewOffMap ) of
                0 ->
                    mnesia:dirty_delete( off_msg_index, UserId );
                _NotZero ->
                    mnesia:dirty_write( off_msg_index, OffMsgIndex#off_msg_index{ 
                                                                    msg_id_list = NewOffMap, 
                                                                    last_update_time = util:longunixtime()  })
            end
    end.

% ----------------------------------------------------------------------------------------------
% 缓存中的数据超时
% 1: 只做数据的标记，并不删除任何数据。等到数据写库成功之后才删除缓存
% ----------------------------------------------------------------------------------------------
do_timeout_event( UserId ) ->
    case mnesia:dirty_read( off_msg_index, UserId ) of
        [] ->
            skip;
        [ OffMsgIndex ] ->
            OldMap = OffMsgIndex#off_msg_index.msg_id_list,
            NewMap =
            dict:map( 
                fun(_K,V) ->
                    #off_msg_detail{msg_id = MsgId,
                                   type = Type,
                                   qos = Qos,
                                   time = Time} = V,
                    case V#off_msg_detail.save_flag of
                        ?SAVE_STATUS_CACHE ->
                            off_msg_mgr:insert ( off_msg_mgr:get_mgr_name(?OFF_MSG_OFF), [ UserId, MsgId, Time, Qos, Type ]),
                            V#off_msg_detail{save_flag = ?SAVE_STATUS_WRITING} ;
                        _OthersStatus ->
                            V
                    end
                end, 
                OldMap),
            mnesia:dirty_write( off_msg_index, OffMsgIndex#off_msg_index{ 
                                                    msg_id_list = NewMap, 
                                                    last_update_time = util:longunixtime() })
    end.



% --------------------------------------------------------------------------------------------------------
% 收到写库驱动成功写完数据库的通知
% 1: 缓存中没有数据，skip
% 2：缓存中有数据。如果已经收到ack,那么删除DB
% --------------------------------------------------------------------------------------------------------
do_save_ok_event( MsgId, UserId ) ->
    case mnesia:dirty_read( off_msg_index, UserId ) of
        [] ->
            skip;
        [ OffIndex ] ->
            case dict:find( MsgId, OffIndex#off_msg_index.msg_id_list) of
                error ->
                    skip;
                {ok, Detail} ->
                    case Detail#off_msg_detail.ack_flag of
                        true ->
                            off_msg_index_lib:delete_one_off_index( UserId, MsgId ),
                            ok;
                        _NoAck ->
                            skip
                    end, 
                    do_delete_cache( UserId, MsgId )
            end
    end.

do_make_send_msg(UserId, DetailList ) ->
    Fun = fun(Detail, MsgAccList) ->
        MsgId = Detail#off_msg_detail.msg_id,
        case off_msg_tmp_cache:get_tmp( MsgId ) of
            [] ->
                MsgAccList;
            OffMsgTmp ->
                #off_msg_tmp{msg_id = MsgId, 
                            from_id = FromId, 
                            topic = Topic,
                            content = Content,
                            type = Type,
                            qos = Qos  } = OffMsgTmp,
                [ {UserId, MsgId, FromId, Topic, Content, Type, min( Detail#off_msg_detail.qos ,Qos) } | MsgAccList ]
        end 
    end,
    lists:foldl(Fun, [], DetailList ).

do_filter_new_maps( NewMap0, ListPre) ->
    Fun = fun( Detail, AccMap) ->
        dict:erase( Detail#off_msg_detail.msg_id, AccMap )
    end,
    lists:foldl(Fun, NewMap0, ListPre ).

% ------------------------------------------------------------------------------------------------------
% 前端请求离线消息事件
% ------------------------------------------------------------------------------------------------------
do_off_msg_event( FromNode, UserId, MsgLimit ) ->
    {OldIndex, CacheMap} =
    case mnesia:dirty_read( off_msg_index, UserId ) of
        [] ->
            { #off_msg_index{ user_id = UserId, msg_id_list = dict:new(), last_update_time = util:longunixtime()}, dict:new() };
        [ OffIndex ] ->
            { OffIndex, OffIndex#off_msg_index.msg_id_list }
    end,

    OffListDB = off_msg_index_lib:get_off_msg_index_from_db( UserId, max( 0 , min(do_get_off_msg_limit(), MsgLimit) - dict:size( CacheMap) )  ),
    FunMerge = fun([ TMsgId0, TType, TQos, TTime ],  AccMaps) ->
        TMsgId1 = integer_to_binary(TMsgId0),
        dict:store( TMsgId1, #off_msg_detail{msg_id = TMsgId1, type = TType, save_flag = ?SAVE_STATUS_CACHE, ack_flag = false,qos = TQos, time = TTime }, AccMaps )
    end,
    NewMaps = lists:foldl( FunMerge, CacheMap, OffListDB ),
    case dict:size( NewMaps ) of
        0 ->
            mnesia:dirty_delete( off_msg_index, UserId );
        _NotZero ->
             

            DetaiList0 = lists:sort( fun do_sort_detail/2, [ TValue || { _TKey, TValue}  <- dict:to_list( NewMaps ) ]),
            {DetaiList, NewMaps1 } =
            case length( DetaiList0 ) > do_get_off_msg_limit() of
                true ->
                    {List_Pre, List_End} = lists:split( length( DetaiList0 ) - do_get_off_msg_limit() , DetaiList0),
                    {List_End, do_filter_new_maps( NewMaps, List_Pre )};
                _OthersEnough ->
                    {DetaiList0, NewMaps}
            end,
   
            mnesia:dirty_write( off_msg_index, OldIndex#off_msg_index{ msg_id_list = NewMaps1 }),

            SendMsgList = do_make_send_msg(UserId, DetaiList ),    
    

            off_msg_ds:delete(off_msg_ds:get_server_name( index ), [ UserId ] ),
            
            cast_svc:cast(  FromNode, emqttd_mm, route_client, [ UserId, SendMsgList ])
    end,
    ok.

% ------------------------------------------------------------------------------------------------------
% 收到im发过来的 ack
% 1： 判断这条消息是否在缓存中。
%     因为收到ack的时候，用户必须在线。所以如果不在，那么认为这条消息有问题。
% 
% 2:  在缓存中，判断当前这条消息的状态。cache状态：
%     + 直接删除 cache
%     + 私聊消息的话，删除 tmp 缓存和数据库
% 
% 3： 如果是 writing 状态的话，标记这条消息的 ack_flag = true
% 4： 收到数据库 drive 发送的消息写库成功回执的时候
%     + 如果 ack_flag 为true 删除DB
%     + 删除 cach
%     + 私聊的消息的话，删除 tmp 缓存和数据库
% ------------------------------------------------------------------------------------------------------
do_im_ack( MsgId, UserId ) ->
   case mnesia:dirty_read( off_msg_index, UserId ) of
        [] ->
            skip;
        [ OffMsgIndex ] ->
            OldMap = OffMsgIndex#off_msg_index.msg_id_list,
            case dict:find( MsgId, OldMap ) of
                error ->
                    skip;
                {ok, Detail} ->
                    case Detail#off_msg_detail.type of
                        <<"p">> ->
                            off_msg_tmp_cache:ack_event( MsgId );
                        _GroupMsg ->
                            skip
                    end,

                    case Detail#off_msg_detail.save_flag of
                        ?SAVE_STATUS_CACHE ->
                            do_delete_cache( UserId, MsgId ),
                            ok;
                        ?SAVE_STATUS_WRITING ->
                            do_delete_cache( UserId, MsgId ),
                            ok;
                        _OthersSkip ->
                            do_delete_cache( UserId, MsgId ),
                            skip
                    end
            end,

            ok
    end.

% -------------------------------------------------------------------------------------------------
% 对消息进行排序，但是不能通过消息的id来。不同的节点产生的消息id不一样。重新开机后也不一样
% -------------------------------------------------------------------------------------------------
do_sort_detail( Detail1, Detail2 ) ->
    case Detail1#off_msg_detail.time == Detail2#off_msg_detail.time of
        true ->
            binary_to_list( Detail1#off_msg_detail.msg_id ) < binary_to_list( Detail2#off_msg_detail.msg_id);
        _NotSame ->
            Detail1#off_msg_detail.time < Detail2#off_msg_detail.time
    end.

% ------------------------------------------------------------------------------------------------
% 离线消息数达到高水位的时候。
% 如果是 1000 - 600 = 400.
% 返回需要删除的数据列表
% ------------------------------------------------------------------------------------------------
do_delete_oldest_off_msg (DetailList, DelNum ) ->
    case length( DetailList ) < DelNum of
        true ->
            [];
        _Ok ->
            DetailList1 = lists:sort( fun do_sort_detail/2, DetailList ),
            {H, _T} = lists:split( DelNum, DetailList1 ),
            H
    end.
    
% ------------------------------------------------------------------------------------------------
% 新消息进来
% 1：直接添加到 cache 的队列中
% 2：如果是私聊的消息，更新缓存的最后更新时间
% ------------------------------------------------------------------------------------------------
do_new_index_msg( UserId, MsgId, Qos, Type, NowTime ) ->
     try
        OffMsgDetail = #off_msg_detail{
                                        msg_id = MsgId,
                                        qos = Qos,
                                        type = Type,
                                        ack_flag = false,
                                        save_flag = ?SAVE_STATUS_CACHE,
                                        time = NowTime
        },
        case mnesia:dirty_read( off_msg_index, UserId) of
            [] ->
                mnesia:dirty_write(#off_msg_index{
                                       user_id = UserId,
                                       last_update_time = NowTime,
                                       msg_id_list = dict:store( MsgId, OffMsgDetail, dict:new() )
                                   });
            [ OffMsgIndex ] ->
                OldOffMap = OffMsgIndex#off_msg_index.msg_id_list,

                % ------------------------------------------------------------------
                % 如果超过高水位的离线消息数限制，那么删除掉那些消息，回退到低水位
                % -----------------------------------------------------------------
                NewOffMap0 =
                case dict:size( OldOffMap ) >= do_get_off_msg_limit_high() of
                    true ->
                        DeleteList = do_delete_oldest_off_msg( 
                                                [ TValue || {_Tkey, TValue} <- dict:to_list( OldOffMap)], 
                                                do_get_off_msg_limit_high() - do_get_off_msg_limit() ),
                        lists:foldl(
                        fun( TDetail0, AccMaps) ->
                            dict:erase( TDetail0#off_msg_detail.msg_id, AccMaps )
                        end, 
                        OldOffMap, DeleteList);
                    _NoExc ->
                        OldOffMap
                end,


                NewOffMap = dict:store( MsgId, OffMsgDetail, NewOffMap0 ),
                NewTime =
                case Type of
                    <<"p">> ->
                        NowTime;
                    _NotP ->
                        OffMsgIndex#off_msg_index.last_update_time
                end,
                mnesia:dirty_write( OffMsgIndex#off_msg_index{ msg_id_list = NewOffMap, last_update_time = NewTime })
        end
    catch
        M:R ->
           lager:error("M:~p R:~p ~n erlangTrace:~p ~n",[M, R, erlang:get_stacktrace()])
    end.

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

sort_msg_test() ->
    Msg1 = #off_msg_detail{ msg_id = <<"1001">>, time = 11 },
    Msg2 = #off_msg_detail{ msg_id = <<"1002">>, time = 11 },
    Msg3 = #off_msg_detail{ msg_id = <<"2001">>, time = 9 },
    Msg4 = #off_msg_detail{ msg_id = <<"3001">>, time = 9 },
    ?assertEqual( [Msg3, Msg4,  Msg1, Msg2], lists:sort( fun do_sort_detail/2, [Msg4,Msg1, Msg2, Msg3]) ).







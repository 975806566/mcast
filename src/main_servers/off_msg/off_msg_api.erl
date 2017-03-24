
-module( off_msg_api ).

-export([
        new_tmp_group_msg/6,
        new_tmp_person_msg/8,
        new_index_msg/6,
        im_ack/2,
        get_hash_node/1,
        get_off_msg_event/3,
        clean_offmsg/1
        ]).

get_hash_node( HashKey ) ->
    do_get_cast_node( HashKey, get_online_off_msg_node_list() ).

% --------------------------------------------------------------------
% @doc 获取离线消息的集群台数
% --------------------------------------------------------------------
get_online_off_msg_node_list() ->
    case util:get_value( <<"off_node_list">>, no_exits ) of
        no_exits ->
            {ok, NodeList} = util:get_env( mcast, off_servers, [node()] ),
            util:set_kv( <<"off_node_list">>, NodeList ),
            NodeList;
        NodeListEts ->
            NodeListEts
    end.

% ---------------------------------------------------------------------
% 获取集群中需要路由的那个节点
% ---------------------------------------------------------------------
do_get_cast_node( HashKey , NodeList ) ->
    case NodeList of
        [] ->
            lager:error(" no off msg list found ~n",[]),
            node();
        _NotNull ->
            NodeId = (binary:first( HashKey ) * 256 + binary:last( HashKey ) rem 23) rem length(NodeList) + 1,
            lists:nth( NodeId, NodeList )
    end.

% ---------------------------------------------------------------------
% 消息路由给 off_msg 服务
% ---------------------------------------------------------------------
-spec do_off_msg_cast( HashKey::binary(), M::atom(), F::any(), A::list() ) ->   ok.
do_off_msg_cast( HashKey, M, F, A) ->
    CastNode = get_hash_node( HashKey ),
    cast_svc:cast(CastNode, M, F, A ).

% ---------------------------------------------------------------------
% @doc 创建群消息的模版
% ---------------------------------------------------------------------
new_tmp_group_msg( MsgId, From, Topic, Content, Qos, NowTime ) ->
    do_off_msg_cast( From, 
                     off_msg_tmp_cache, 
                     new_tmp_group_msg, 
                     [ MsgId, From, Topic, Content, Qos, NowTime]
                   ).

% ---------------------------------------------------------------------
% @doc 创建私聊消息的模版 
% ---------------------------------------------------------------------
new_tmp_person_msg(ToId, MsgId, From, Topic, Content, Qos, ApnsInfo, NowTime ) ->
    do_off_msg_cast( ToId,
                     off_msg_tmp_cache,
                     new_tmp_person_msg,
                     [ToId, MsgId, From, Topic, Content, Qos, ApnsInfo, NowTime ] ).

% --------------------------------------------------------------------
% @doc 创建离线消息的索引
% --------------------------------------------------------------------
new_index_msg(UserId, MsgId, Qos, Type, ApnsInfo, NowTime ) ->
    do_off_msg_cast( UserId,
                     off_msg_index_cache,
                     new_index_msg,
                     [ UserId, MsgId, Qos, Type, ApnsInfo, NowTime ] ).

% --------------------------------------------------------------------
% @doc ack 反馈
% --------------------------------------------------------------------
im_ack( MsgId, UserId ) ->
    do_off_msg_cast( UserId,
                     off_msg_index_cache,
                     im_ack,
                     [  MsgId, UserId ] ).

% --------------------------------------------------------------------
% @doc 前端请求获取离线消息
% --------------------------------------------------------------------
get_off_msg_event( FromNode, UserId, MsgLimit ) ->
    do_off_msg_cast( UserId,
                     off_msg_index_cache,
                     get_off_msg_event,
                     [FromNode, UserId, MsgLimit] ).

% --------------------------------------------------------------------
% @doc 清除离线消息
% --------------------------------------------------------------------
clean_offmsg( UserId ) ->
    do_off_msg_cast( UserId,
                     off_msg_index_cache,
                     clean_offmsg,
                     [ UserId ] ).




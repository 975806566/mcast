
-module( off_msg_index_lib ).


-export([
         get_off_msg_index_from_db/2,
         delete_one_off_index/2,
         get_off_msg_index_count/1
        ]).

-include("off_msg.hrl").

get_off_msg_index_from_db( _UserId, 0 ) ->
    [];
get_off_msg_index_from_db( UserId, Num ) ->
    case mysql_poolboy:query( ?OFF_MSG_MYSQL_POOL, "select msg_id,type,qos,time from off_msg where user_id = ? order by time desc,msg_id desc limit ? ", [UserId, Num] ) of
         {ok, _Fields, ResList} ->
            ResList;
         _OthersRes ->
            []
    end.

get_off_msg_index_count( UserId ) ->
     case mysql_poolboy:query( ?OFF_MSG_MYSQL_POOL, "select count(*) from off_msg where user_id = ?", [ UserId ] ) of
         {ok, _Fields, [[Count]]} ->
            Count;
         _OthersRes ->
            0
    end.



% -------------------------------------------------------------------
% @doc 删除单条的 off_index 数据
% --------------------------------------------------------------------
delete_one_off_index( UserId, MsgId ) ->
    Sql = "delete from off_msg where user_id = ? and msg_id = ?",
    mysql_poolboy:query( ?OFF_MSG_MYSQL_POOL, Sql, [UserId, MsgId] ).







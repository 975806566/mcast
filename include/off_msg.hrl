
-ifndef(OFF_MSG_HRL).

-define( OFF_MSG_HRL, true ).

-define( OFF_MSG_MGR_ETS,       off_msg_mgr_ets).

-define( OFF_MSG_STATUS_IDLE,   0).             % 工作进程状态空闲
-define( OFF_MSG_STATUS_BUSY,   1).             % 工作进程状态繁忙

-define( OFF_MSG_MYSQL_POOL,    mysql_pool2).   % 缓冲池名字

-define( OFF_MSG_TMP,           "tmp").         % 写表 msg_tmp
-define( OFF_MSG_OFF,           "off").         % 写表 off_msg

-define( SAVE_STATUS_CACHE,     0).
-define( SAVE_STATUS_WRITING,   1).
-define( SAVE_STATUS_WRITED,    2).


% ----------------------------------------------------------------------------
% @doc 离线消息的模版
% ----------------------------------------------------------------------------
-record( 
            off_msg_tmp,{   
            msg_id = 0,                 % 唯一ID 64位
            from_id = 0,                % 短ID16位
            topic = "",                 % 主题
            content = "",               % 消息内容
            type = 0,                   % type
            qos = 0,                    % 消息级别
            save_flag = false,          % 消息是否已经保存的标识符
            time = 0,                   % 消息的发送时间
            last_update_time = 0        % 消息导入到内存的时间
       }).

% ----------------------------------------------------------------------------
% @doc 未接受消息的映射表
% ----------------------------------------------------------------------------
-record( off_msg_index, {
            user_id = 0,                        % 用户 id
            msg_id_list = dict:new(),           % 用户 id 对应的消息列表
            last_update_time = 0                % 最后更新的时间
        }).

% ----------------------------------------------------------------------------
% @doc 单挑映射表的详细信息
% ----------------------------------------------------------------------------
-record( off_msg_detail, {
               msg_id = "12313",
               save_flag = 0,
               ack_flag = false,
               qos = 0,
               type = <<"g">>,
               time = 1203021
}).

-endif.

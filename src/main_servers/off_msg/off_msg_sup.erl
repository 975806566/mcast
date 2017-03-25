% -----------------------------------------------------------------------------
% @doc 管理离线消息的所有服务
% -----------------------------------------------------------------------------


-module( off_msg_sup ).

-export([start_link/0]).

-export([init/1]).

-include("off_msg.hrl").
-include("emqttd.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


do_get_tmp_header() ->
    "replace into `msg_tmp`(msg_id,from_id,topic,content,type,qos,time) values ".

do_get_tmp_tail() ->
     ",(?,?,?,?,?,?,?)".

do_get_off_header() ->
     "insert into `off_msg`(user_id,msg_id,`time`,`qos`,`type`) values ".

do_get_off_tail() ->
     ",(?,?,?,?,?)".


init([]) ->
    application:ensure_all_started( mnesia ),
    application:ensure_all_started( gproc ),
    new_off_msg_mgr_ets(),
    {ok, OffMsgNodes} = util:get_env( mcast, off_servers, [node()] ),
    extra_db_nodes( OffMsgNodes ),    
    create_all_tables( OffMsgNodes ),
    PoolOptionsDefault  = [{size, 10}, {max_overflow, 20}],
    MySqlOptionsDefault = [{user, "root"}, {password, "123456"}, {database, "im_db"}],


    {ok, PoolOptions} = util:get_env(mcast,  mysql_pooloptions, PoolOptionsDefault),
    {ok, MySqlOptions} = util:get_env(mcast, mysql_options, MySqlOptionsDefault),
    
    % -----------------------------------------------------------------------------
    % mysql 写数据库的驱动服务
    % -----------------------------------------------------------------------------
    MysqlDrive = mysql_poolboy:child_spec(?OFF_MSG_MYSQL_POOL, PoolOptions, MySqlOptions),

    % ----------------------------------------------------------------------------
    % mysql 写入msg_tmp数据的批量服务
    % ----------------------------------------------------------------------------
    OffMgrTmp =  {  off_msg_mgr_tmp ,
                  { off_msg_mgr, start_link, [ ?OFF_MSG_TMP, do_get_tmp_header(), do_get_tmp_tail() ]},
                  permanent, 10000, worker, [ off_msg_mgr_tmp ]} ,
    
    % ---------------------------------------------------------------------------
    % mysql 写入off_msg数据的批量服务
    % ---------------------------------------------------------------------------
    OffMgrOff =  {  off_msg_mgr_off ,
                  { off_msg_mgr, start_link, [ ?OFF_MSG_OFF, do_get_off_header(), do_get_off_tail() ]},
                  permanent, 10000, worker, [ off_msg_mgr_off ]} ,
    
    % --------------------------------------------------------------------------
    % 定时清理  tmp 缓存的服务 
    % --------------------------------------------------------------------------
    OffMsgTmpCCS =  { off_msg_tmp_ccs,
                  {off_msg_tmp_ccs , start_link, []},
                  permanent, 10000, worker, [ off_msg_tmp_ccs ]} ,

    % -------------------------------------------------------------------------
    % 定时清理 index 缓存的服务
    %--------------------------------------------------------------------------
    OffMsgIndexCCS =  { off_msg_index_ccs,
                  {off_msg_index_ccs , start_link, []},
                  permanent, 10000, worker, [ off_msg_index_ccs ]} ,

    % -------------------------------------------------------------------------
    % 删除 tmp 数据的服务
    % -------------------------------------------------------------------------
    OffMsgTmpDS =  { off_msg_tmp_ds,
                  {off_msg_ds , start_link, [off_msg_ds:get_server_name(tmp), fun do_make_sql_tmp/1]},
                  permanent, 10000, worker, [ off_msg_tmp_ds ]} ,

    % -------------------------------------------------------------------------
    % 删除 index 数据的服务
    % -------------------------------------------------------------------------
    OffMsgIndexDS =  { off_msg_index_ds,
                  {off_msg_ds , start_link, [off_msg_ds:get_server_name(index), fun do_make_sql_index/1]},
                  permanent, 10000, worker, [ off_msg_index_ds ]} ,

    % -------------------------------------------------------------------------
    % 维护单进程写 tmp 数据的服务群
    % -------------------------------------------------------------------------
    OffMsgTmpCache = {off_msg_tmp_sup, {off_msg_tmp_sup, start_link, []}, permanent, 5000, supervisor, [ off_msg_tmp_sup ]},

    % ------------------------------------------------------------------------
    % 维护单进程写 index 数据的服务群
    % ------------------------------------------------------------------------
    OffMsgIndexCache = {off_msg_index_sup, {off_msg_index_sup, start_link, []}, permanent, 5000, supervisor, [ off_msg_index_sup ]},

    {ok, {{one_for_all, 10, 100}, [MysqlDrive, OffMsgTmpDS, OffMsgIndexDS, OffMgrTmp, OffMgrOff,  OffMsgTmpCCS, OffMsgIndexCCS, OffMsgTmpCache, OffMsgIndexCache ]}}.

new_off_msg_mgr_ets() ->
    ets:new( ?OFF_MSG_MGR_ETS, [named_table, public, set]).

% ------------------------------------------------------------
% @doc 创建mnesia集群的表
% ------------------------------------------------------------
create_all_tables ( Nodes ) ->

    util:create_or_copy_table( off_msg_tmp,   [{ram_copies, Nodes}, {attributes, record_info(fields, off_msg_tmp)}],    ram_copies),
    util:create_or_copy_table( off_msg_index, [{ram_copies, Nodes}, {attributes, record_info(fields, off_msg_index)}],  ram_copies),
    util:create_or_copy_table( users,         [{ram_copies, Nodes}, {attributes, record_info(fields, users)}],          ram_copies),
    util:create_or_copy_table( user_info,     [{ram_copies, Nodes}, {attributes, record_info(fields, user_info)}],      ram_copies),

    mnesia:wait_for_tables([ off_msg_tmp, off_msg_index, users, user_info ], 600 * 1000).

% ------------------------------------------------------------
% @doc 连接上需要集群的 nodes
% ------------------------------------------------------------
extra_db_nodes( Nodes ) ->
    mnesia:change_config(extra_db_nodes, Nodes).

do_make_sql_tmp( Block ) ->
    Fun = fun( [MsgId], AccSql ) ->
        "delete from msg_tmp where msg_id = '" ++ binary_to_list(MsgId) ++ "';" ++ AccSql
    end,
    lists:foldl(Fun, "", Block).

do_make_sql_index( Block ) ->
    Fun = fun( [ UserId ], AccSql ) ->
        "delete from off_msg where user_id = '" ++ binary_to_list(UserId) ++ "';" ++ AccSql
    end,
    lists:foldl(Fun, "", Block).



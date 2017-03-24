-module(mcast_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

% --------------------------------------------------------------------
% @doc 判断自己是否位于服务器列表中
% --------------------------------------------------------------------
check_in_list( {ok, NodeList} ) ->
    lists:member( node(), NodeList ).

init([]) ->
    do_create_glb_kv(),

    % ------------ 节点管理服务 以及 cast supervisor ---------- % 
    NodeMgrSpec = ?CHILD(cast_node_mgr, worker),
    CastSvcSpec = ?CHILD(cast_svc_sup, supervisor),
    % --------------------------------------------------------- %
    BaseMCastServers = [NodeMgrSpec, CastSvcSpec],


    % ------------- 其他的服务 ----------- %
    %                                                           %
    % ------------------------------------ %


    % --------------------------------------------------------- %
    % 离线消息服务
    % --------------------------------------------------------- %
    ExtraServers0 = [],
    ExtraServers1 =
    case check_in_list( util:get_env(mcast, off_servers, []) ) of
        true ->
            [?CHILD( off_msg_sup, supervisor ) | ExtraServers0];
        false ->
            ExtraServers0
    end,

    % --------------------------------------------------------- %
    % 离线消息 id 生产服务
    % --------------------------------------------------------- %
    ExtraServers2 =
    case check_in_list( util:get_env(mcast, off_id_servers, []) ) of
        true ->
            [?CHILD( off_msg_id_svc, worker ) | ExtraServers1];
        false ->
            ExtraServers1
    end,

    ExtraServers3 = [ ?CHILD(svc_monitor_svc, worker) | ExtraServers2 ],

    ExtraServers = ExtraServers3,
    {ok, { {one_for_one, 5, 10}, BaseMCastServers ++ ExtraServers } }.

% ------------------------------------------------------------
%  创建全局的 kv 字典
% ------------------------------------------------------------
do_create_glb_kv() ->
    ets:new(kv, [named_table, set, public ]).







-module( off_msg_tmp_sup ).

-behaviour(supervisor).

-export([init/1]).

-export([start_link/0]).

-include("emqttd.hrl").
-include("off_msg.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    load_all_users( ),
    Schedulers = erlang:system_info(schedulers) * 4,
    gproc_pool:new( off_msg_tmp_cache:pool(), hash, [{size, Schedulers}] ),
    Children = lists:map(
                 fun(I) ->
                    Name = {off_msg_tmp_cache, I},
                    gproc_pool:add_worker( off_msg_tmp_cache:pool() , Name, I),
                    {Name, {off_msg_tmp_cache, start_link, [I]},
                                permanent, 10000, worker, [ off_msg_tmp_cache ]}
                 end, lists:seq(1, Schedulers)),
    {ok, {{one_for_all, 10, 100}, Children}}.

% ------------------------------------------------------------------
% @doc 数据库中的 users 数据，导入到 mnesia 中
% ------------------------------------------------------------------
load_all_users() ->
    mysql_poolboy:with( ?OFF_MSG_MYSQL_POOL, fun (Pid) ->
            { ok, _Fileds, Res } =  mysql:query(Pid, "select * from user_info;" ),
            [ begin
                      UserId = util:a2b( UserId0 ),
                      Appkey = util:a2b( AppKey0 ),
                      UserName = util:a2b( UserName0 ),
                      mnesia:dirty_write(users, #users{ user_id = UserId,
                                                        appkey_id = Appkey,
                                                        username = UserName,
                                                        name = UserName
                                                      }),
                      mnesia:dirty_write(user_info, #user_info{
                                            id = UserId,
                                            appkey = Appkey
                                          })
              end
             || [ UserId0, AppKey0, UserName0 ] <- Res ]
    end).




-module( off_msg_tmp_sup ).

-behaviour(supervisor).

-export([init/1]).

-export([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
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




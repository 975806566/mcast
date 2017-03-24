-module( cast_svc_sup ).

-behaviour(supervisor).

-export([
         start_link/0,
         init/1,
         start_child/1
        ]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ChildSpec = {undefined,
                 {cast_svc, start_link, []},
                 transient,
                 10000,
                 worker,
                 [cast_svc]},

    {ok, {{simple_one_for_one, 10, 3600}, [ChildSpec]}}.

start_child( _Node ) ->
    ok.


-module(svc_monitor_svc).

-behaviour(gen_server).

%% API functions
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API functions
%%%===================================================================

-define( MONITOR_TIMER,     5 * 1000).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    util:print(" svc_monitor_svc start ~n",[]),
    erlang:send_after(  0, self(), { timer_ticker }),
    {ok, #state{}}.

timer_ticker() ->
    erlang:send_after( ?MONITOR_TIMER, self(), { timer_ticker }).

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

do_update_im_server( Nodes ) ->
    OldNodes0 = ets:match( kv, {{im, '$1'}, '_'}),
    OldNodes1 = lists:flatten( OldNodes0 ),
    
    %-% 获取在线的 im 节点。
    FunIm = 
    fun( E, AccImNodes ) ->
        case util:a2l( E ) of
            [$i, $m, $_, $s, $e, $r, $v, $e, $r | _Left] ->
                [ E | AccImNodes ];
            _NotMatch ->
                AccImNodes
        end
    end,
    OnlineImNodes = lists:foldl( FunIm, [], Nodes), 

    %-% 删掉不在线的 IM 节点。
    FunDel = fun( Node, _Acc) ->
        case lists:member( Node, OnlineImNodes ) of
            true ->
                skip;
            _False ->
                ets:delete( kv, {im, Node} )
        end
    end,
    lists:foldl( FunDel, [], OldNodes1 ),

    [ ets:insert(kv, {{im, TNode }, true }) || TNode <- OnlineImNodes ],

    ok.

handle_info( {timer_ticker}, State) ->
    OffNodeList = util:get_value_cache( mcast, off_servers, []),
    OnlineNodes = [ node() | nodes() ],

    % ---------------------------------------------------
    %  找出在线的离线服务器
    % ---------------------------------------------------
    Fun = fun( OffNode, AccNodeList ) ->
        case lists:member( OffNode, OnlineNodes ) of
            true ->
                [ OffNode | AccNodeList ];
            _NotTrue ->
                AccNodeList
        end
    end,
    OnlineOffNodes = lists:foldl( Fun, [], OffNodeList ),

    util:set_kv( <<"off_node_list">>, OnlineOffNodes ),

    do_update_im_server( OnlineNodes ),

    timer_ticker(),
    {noreply, State}; 

handle_info(_Info, State) ->
    util:print("handle info undefined info ~p ~n", [ _Info ]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


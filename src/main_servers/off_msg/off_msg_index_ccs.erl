-module(off_msg_index_ccs).

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

-include("off_msg.hrl").

-define( MAX_CACHE_TIME,        1 * 60 * 1000).          % cache 的时间为半个小时 
-define( CACHE_CLEAN_INTERVAL,  5 * 1000).          % 执行的时间间隔为 5 分钟

% --------------------------------------------------------------------
% @doc 删除过期的缓存。
% --------------------------------------------------------------------
clear_timeout_cache() ->
	erlang:send_after( ?CACHE_CLEAN_INTERVAL, self(), {clear_cache}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    clear_timeout_cache(),
    {ok, #state{}}.

% --------------------------------------------------------------------
% 判断是否已经超时。
% 只有那些保存的状态为cache的
% 并且超时的数据才会进行删除
% --------------------------------------------------------------------
do_check_time_out(NowTime, OffMsgIndex) ->
    abs( NowTime - OffMsgIndex#off_msg_index.last_update_time ) >= ?MAX_CACHE_TIME.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info( {clear_cache}, State ) ->
    Now = util:longunixtime(),
    KeyList = mnesia:dirty_all_keys( off_msg_index ),
    Fun = fun( UserId, _AccList) ->
        case mnesia:dirty_read( off_msg_index, UserId ) of
            [] ->
                skip;
            [ OffMsgIndex ] ->
                case do_check_time_out(Now, OffMsgIndex ) andalso off_msg_api:get_hash_node( UserId ) =:= node() of
                    true ->
                        off_msg_index_cache:timeout_event( OffMsgIndex#off_msg_index.user_id );
                    _NotTimeOut ->
                        skip
                end
        end
    end,
    lists:foldl( Fun, [], KeyList ),
    clear_timeout_cache(),
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

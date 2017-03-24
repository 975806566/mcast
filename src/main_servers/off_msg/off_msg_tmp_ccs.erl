-module(off_msg_tmp_ccs).

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

-define( MAX_CACHE_TIME,        60 * 1000).
-define( CACHE_CLEAN_INTERVAL,  5 * 1000).

-define( FORCE_GC_TIME,     12 * 60 * 1000).
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% --------------------------------------------------------------------
% @doc 删除过期的缓存。
% --------------------------------------------------------------------
clear_timeout_cache() ->
	erlang:send_after( ?CACHE_CLEAN_INTERVAL, self(), {clear_cache}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    force_gc(),
    clear_timeout_cache(),
    {ok, #state{}}.


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

force_gc() ->
    erlang:send_after( ?FORCE_GC_TIME, self(), {force_gc}).

handle_info({force_gc}, State) ->
    force_gc(),
    spawn(fun() -> erlang:garbage_collect( self() ) end),
    {noreply, State};
handle_info( {clear_cache}, State ) ->
    Now = util:longunixtime(),
    KeyList = mnesia:dirty_all_keys( off_msg_tmp ),
    Fun = fun( MsgId, _AccList) ->
        case mnesia:dirty_read( off_msg_tmp, MsgId ) of
            [] ->
                skip;
            [ OffMsgRc ] ->
                case do_check_time_out(Now, OffMsgRc ) andalso off_msg_api:get_hash_node(MsgId) =:= node() of
                    true ->
                        off_msg_tmp_cache:delete_tmp( OffMsgRc#off_msg_tmp.msg_id  );
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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
do_check_time_out(NowTime, OffMsgRc) ->
    abs( NowTime - OffMsgRc#off_msg_tmp.last_update_time ) >= ?MAX_CACHE_TIME andalso OffMsgRc#off_msg_tmp.save_flag.



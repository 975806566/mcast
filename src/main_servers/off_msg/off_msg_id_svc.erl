% -----------------------------------------------------------------------
% @author chenyiming
% @doc 用于生产离线消息的唯一 id
% -----------------------------------------------------------------------

-module(off_msg_id_svc).

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

-export([
        get_msg_id/0
        ]).

-record(state, {
                     node_id = 1,           % 节点序列号
                     restart_seq = 1,       % 机器重启序列号 
                     msg_id = 1             % 消息序列号
                 }).

-include_lib("eunit/include/eunit.hrl").


-define(RESTART_FILE,       "off_restart_seq").
% ------------------------------------------------------------------
% ID 组成结构 高位到低位  
%               1-4(位)   5-20位   21-64位
%               ----      -----    ------
%               |           |         |
%            机器编号    开机序列号 消息id号
% ------------------------------------------------------------------

-export([   
            get_off_server_id/0
        ]).

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

get_msg_id() ->
    gen_server:call( ?MODULE, {get_msg_id} ).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

write_init_restart_seq(Seq) when is_integer(Seq) orelse is_list(Seq) ->
    Seq1 = util:a2b(Seq),
    Bin = <<"{ off_restart_seq, ", Seq1/binary, "}." >>,
    file:write_file(?RESTART_FILE, Bin),

    binary_to_integer( Seq1 ).

% ---------------------------------------------------------------------
% @doc 获取开机后的序列号
% ---------------------------------------------------------------------
get_restart_seq() ->
    case filelib:is_file(?RESTART_FILE) andalso ( not filelib:is_dir(?RESTART_FILE)) of
        true ->
            case file:consult(?RESTART_FILE) of
                {ok, Terms} ->
                    OldSeq = proplists:get_value(off_restart_seq, Terms, 1),
                    NewSeq = 
                    case OldSeq of
                        65535 ->
                            1;
                        _NotMax ->
                            OldSeq + 1
                    end,
                    write_init_restart_seq( NewSeq );
                _ErrorContent ->
                    write_init_restart_seq( 1 )
            end;
        _false ->
            write_init_restart_seq( 1 )
    end.

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
    {ok, #state{ restart_seq  = get_restart_seq(), node_id = get_off_server_id(), msg_id = 1}}.


do_get_off_server_id([ _H | _T], _H, NowIndex) ->
    NowIndex;
do_get_off_server_id([],_Node,  _NowIndex) ->
    1;
do_get_off_server_id([ _H | T ], Node, NowIndex ) ->
    do_get_off_server_id( T, Node, NowIndex + 1).

get_off_server_id() ->
    {ok, OffNodes} = util:get_env( mcast, off_id_servers, [ node() ]),
    do_get_off_server_id( OffNodes, node(), 1).

handle_call( {get_msg_id}, _From, State) ->
    OldMsgId = State#state.msg_id,
    NewId = integer_to_list( (State#state.node_id bsl 60) + (State#state.restart_seq  bsl 44) + OldMsgId ),
    {reply, list_to_binary( NewId ), State#state{msg_id = OldMsgId + 1}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
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
node_id_test() ->
    ?assertEqual(1,  do_get_off_server_id([a,b,c,d],   9, 1)),
    ?assertEqual(2,  do_get_off_server_id([a,b,c,d,b,a], b, 1)),
    ?assertEqual(3,  do_get_off_server_id([a,b,c,d,b,a], c, 1)),
    ?assertEqual(1,  do_get_off_server_id([a,b,c,d,b,a], a,1 )),
    ?assertEqual(1,  do_get_off_server_id([a], a, 1)),

    ok.




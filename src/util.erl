
-module(util).


-export([
			a2l/1,
            print/2,
			unixtime/0,
			longunixtime/0,
            ch_string/1,
            splite_lists/2,
            ceil/1,
            get_env/3,
			a2b/1,
            get_value/2,
            set_kv/2,
            get_real_uid/2,
            get_real_uid_list/1,
            get_uname_by_ruid/1,
            get_value_cache/3,
            create_or_copy_table/3
		]).


-include_lib("eunit/include/eunit.hrl").

create_or_copy_table(TableName, Opts, Copy) ->
    case mnesia:create_table(TableName, Opts) of
        {aborted, {already_exists, _}} ->
            mnesia:add_table_copy(TableName, node(), Copy);
        _Others ->
            % ?ERROR_MSG("Others:~p ~n",[_Others]),
            skip
    end.

do_get_convert_module() ->
     case get_value( convert_module, undefined ) of
        undefined ->
            {ok, CMConfig} = get_env( emqttd, convert_module, emqttd_convert ),
            set_kv( convert_module, CMConfig ),
            CMConfig;
        V ->
            V
    end.

get_uname_by_ruid( RUid ) ->
    ConverModule = do_get_convert_module(),
    ConverModule:get_uname_by_ruid( RUid ).

get_real_uid( Sdk , Username ) ->
    ConverModule = do_get_convert_module(),
    ConverModule:get_real_uid( Sdk, Username ).

get_real_uid_list( Username ) ->
    ConverModule = do_get_convert_module(),
    ConverModule:get_real_uid_list( Username ).

set_kv(K, V) ->
    ets:insert(kv, {K, V}).

get_value_cache( App, Key, Default ) ->
    case get_value( Key, '$not_defined') of
        '$not_defined' ->
            {ok, Value} = get_env( App, Key, Default ),
            set_kv( Key, Value ),
            Value;
        EtsValue ->
            EtsValue
    end.
   
% --------------------------------------------------------------
% @doc 获取 kv 中的 value
% --------------------------------------------------------------
get_value( Key, DefaultValue ) ->
    case ets:lookup( kv, Key ) of
        [] ->
            DefaultValue;
        [{_K,V}] ->
            V
    end.

% ---------------------------------------------------------------
% @doc 获取系统配置
% ---------------------------------------------------------------
-spec get_env( App::atom(), Key::atom(), Default::atom() ) -> {ok , Res::any()}.
get_env(App, Key, Default) ->
    case application:get_env(App, Key) of
    {ok, Value} ->
            {ok, Value};
        _ ->
            {ok, Default}
    end.

% --------------------------------------------------------------
%  @doc 将列表均等切分
% --------------------------------------------------------------
splite_lists(List, Num) ->
    Len = length(List),
    SubLen = ceil(Len / Num),
    do_splite_lists(SubLen, List, [], Num).

do_splite_lists(_SubLen, _LeftList, AccList, 0) ->
    AccList;
do_splite_lists(SubLen, LeftList, AccList, Num) ->
    case length(LeftList) < SubLen of
        true ->
            do_splite_lists(SubLen, [], [ LeftList | AccList], Num - 1);
        _Continue ->
            {Seg, LeftList1} = lists:split(SubLen, LeftList),
            do_splite_lists(SubLen, LeftList1, [Seg | AccList], Num - 1)
    end.

ceil(N) ->
     T = trunc(N),
     case N == T of
         true  -> T;
         false -> 1 + T
     end.

% -------------------------------------------------------------
% @doc 用于 test 工程内部的打印，绿色展示
% -------------------------------------------------------------
print(A, B) ->
    C =
    case os:type() of
        {unix,linux} ->
            "\033[32m" ++ A ++ "\033[0m";
        _Others ->
            A
    end,
    lager:info("--------- " ++ C, B).

% -------------------------------------------------------------
%	@doc all to list
% -------------------------------------------------------------
a2l(X) when is_integer(X)	-> integer_to_list(X);
a2l(X) when is_float(X)	-> float_to_list(X);
a2l(X) when is_atom(X)	-> atom_to_list(X);
a2l(X) when is_binary(X)	-> binary_to_list(X);
a2l(X) when is_list(X)	-> X.

% -------------------------------------------------------------
%	@doc all to binary
% -------------------------------------------------------------
a2b(X) ->
	L = a2l(X),
	list_to_binary(L).

% -------------------------------------------------------------
%	@doc unixtime stamp
% -------------------------------------------------------------
unixtime() ->
	{M,	S, _} =	os:timestamp(),
	M *	1000000	+ S.

% -------------------------------------------------------------
%	@doc unixtime stamp long
% -------------------------------------------------------------
longunixtime() ->
	{M,	S, Ms} = os:timestamp(),
	(M * 1000000000000 + S * 1000000 + Ms) div 1000.

% -------------------------------------------------------------
% @doc 中文转换成二进制
% -------------------------------------------------------------
ch_string(String) ->
    a2l(unicode:characters_to_binary(String)).

ceil_test() ->
    ?assertEqual( 10,   ceil(9.1) ),
    ?assertEqual( 9,    ceil(9.0) ),
    ?assertEqual( 12,   ceil(11.99)).

splite_test() ->
    L0 = [1, 2, 3, 4, 5],
    R0 = lists:sort( splite_lists(L0,3)),
    ?assertEqual([ [1, 2],[3, 4], [5] ], R0),
    ?assertEqual([ [], [] ,[], [1],[2],[3],[4],[5] ], lists:sort( splite_lists(L0, 8)) ),
    ?assertEqual([ [1, 2], [3, 4]],  lists:sort( splite_lists( [1,2,3,4], 2))  ),
    ?assertEqual( [ [], [], [] ], splite_lists([], 3)).
    

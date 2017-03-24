%% @author Administrator
%% @doc @todo attention please。 这个队列放在进程字典里面，使用前请确认具体的使用方法。


-module(my_queue).

%% ====================================================================
%% API functions
%% ====================================================================
-export([
		 	new/0,
            new/1,
			empty/0,
            full/0,
            full/1,
			in/1,
            in/2,
			out/0,
            in_block/1,
			size/0
		 ]).



%% ====================================================================
%% Internal functions
%% ====================================================================


-include_lib("eunit/include/eunit.hrl").

-define(MAX_BLOCK_SIZE,	1000).                      % 每个 Block 块的最大尺寸
-define(MAX_SLOT,       1000).                      % 总共有多少个 Block 块
-define(MAX_SLOT_KEY,   '$my_queue_max_lot_key').   % 存储最大的 slot
-define(QHD,     '$my_queue_header').               % 队列的头和尾
-define(QED,     '$my_queue_end').
-define(SIZE_KEY,       '$my_queue_size_key').      % mysql 的size

do_add_size( Num ) ->
    OldSize = get(?SIZE_KEY),
    put( ?SIZE_KEY, OldSize + Num ).

do_get_size( ) ->
    get(?SIZE_KEY).

do_set_header( Num ) when is_integer( Num ) ->
    put( ?QHD, Num ).

do_get_header( ) ->
    get( ?QHD ).

do_set_ender( Num ) when is_integer( Num ) ->
    put( ?QED, Num).

do_get_ender( ) ->
    get( ?QED ).

do_set_max_slot( Num ) when is_integer( Num ) ->
    put( ?MAX_SLOT_KEY, Num ).

get_index( Num ) ->
    list_to_atom( "$block_key_" ++ integer_to_list(Num) ).

do_get_block(Num) ->
    get( get_index(Num) ).

do_set_block( Num, Value) ->
    put( get_index( Num ), Value).

do_get_maxslot() ->
    get( ?MAX_SLOT_KEY ).

new() ->
    new( ?MAX_SLOT ).

new( MaxSlot ) ->
    do_set_header( 1 ),
    do_set_ender( 1 ),
    do_set_max_slot( MaxSlot ),
    put( ?SIZE_KEY, 0),
    Fun = fun( Num ) ->
        do_set_block( Num, [] )
    end,
    lists:foreach( Fun,  lists:seq(1, MaxSlot) ).

empty() ->
    Hd = do_get_header(),
    Ed = do_get_ender(),
	Hd =:= Ed andalso do_get_block( Hd ) =:= [].

full() ->
    full(?MAX_BLOCK_SIZE).

full(MaxBlockSize) ->
    Hd = do_get_header(),
    Ed = do_get_ender(),
    MaxSlot = do_get_maxslot(),
    ((Ed + MaxSlot - Hd) rem MaxSlot) =:= (MaxSlot - 1) andalso length(do_get_block( Ed )) =:= MaxBlockSize.

full_slot() ->
    Hd = do_get_header(),
    Ed = do_get_ender(),
    MaxSlot = do_get_maxslot(),
    ((Ed + MaxSlot - Hd) rem MaxSlot) =:= (MaxSlot - 1).

do_get_first_block() ->
	Ed = do_get_ender(),
    do_get_block( Ed ).

do_check_block_full(Block) ->
	length(Block) >= ?MAX_BLOCK_SIZE.
	
do_check_block_full(Block, BlockSize) ->
	length(Block) >= BlockSize.

in(I, BlockSize) ->
    FirstBlock = do_get_first_block(),
    Ed = do_get_ender(),

	case do_check_block_full(FirstBlock, BlockSize) of
		true ->
			case full(BlockSize) of
                true ->
                    false;
                _NotFull ->
                    NewEd = (Ed rem do_get_maxslot()) + 1,
                    do_set_ender(NewEd),
                    do_add_size(1),
                    do_set_block( NewEd , [I])                   
            end;
		_NotFull ->
                        do_add_size(1),
			do_set_block( Ed, [I | FirstBlock])
	end.

do_get_next_slot(St) ->
    (St rem do_get_maxslot()) + 1.

in(I) ->
    FirstBlock = do_get_first_block(),
    Ed = do_get_ender(),
	case do_check_block_full(FirstBlock) of
		true ->
			case full() of
                true ->
                    false;
                _NotFull ->
                    NewEd = (Ed rem do_get_maxslot()) + 1,
                    do_set_ender(NewEd),
                    do_add_size(1),
                    do_set_block( NewEd , [I])
            end;
		_NotFull ->
                        do_add_size(1),
			do_set_block( Ed, [I | FirstBlock])
	end.

in_block(BlockList) ->
    Ed = do_get_ender(),
    case full_slot() of
        true ->
            false;
        _NotFull ->
            NewEd = (Ed rem do_get_maxslot()) + 1,
            do_set_ender(NewEd),
            do_add_size(length(BlockList)),
            do_set_block( NewEd , BlockList)
    end.

out() ->
    case empty() of
        true ->
            [];
        _NotEmpty ->
            St = do_get_header(),
            NewSt = 
                case do_get_ender() of
                    St ->
                        St;
                    _NotSame ->
                        (St rem do_get_maxslot()) + 1
                end,
            do_set_header( NewSt ),
            RtBlock = do_set_block( St , []),
            do_add_size( - length(RtBlock) ),
            RtBlock
    end.
     
do_size(St, St, Sum) ->
    Sum + length(do_get_block( St ));
do_size(St, Ed, Sum) ->       
    do_size( do_get_next_slot(St), Ed, Sum + length(do_get_block( St ))).


real_size() ->
    do_size(do_get_header(), do_get_ender(), 0).

size() ->
    do_get_size().

% show_time() ->
% io:format("~p ~n",[now()]).

do_get_h_e_b(Index) ->
    {do_get_header(), do_get_ender(), do_get_block(Index) }.

size_test() ->
    new(3),
    do_set_header(3),
    do_set_ender(1),
    do_set_block( 3, [1 ,2, 3]),
    do_set_block( 1, [4 ,5]),
    do_set_block( 2, [6]),
    do_add_size( 5 ),
    ?assertEqual(5, real_size()),
    
    new(4),
    do_set_header(2),
    do_set_ender(3),
    do_set_block( 2, [1, 2]),
    do_set_block( 3, [3]),
    ?assertEqual(3, real_size()),

    new(3),
    in(1, 2),
    in(2, 2),
    ?assertEqual(2, size()),
    in(3, 3),
    ?assertEqual(3, size()),
    ok.

out_test() ->
    new(2),
    in(1, 3),
    in(2, 3),
    in(3, 3),
    in(4, 3),
    in(5, 3),
    ?assertEqual([3, 2, 1], out()),
    ?assertEqual([5, 4], out()),
    ?assertEqual([], out()),
    
    new(3),
    do_set_header(3),
    do_set_ender(2),
    do_set_block( 1, [4, 5, 6] ),
    do_set_block( 2, [7, 8, 9] ),
    do_set_block( 3, [1, 2, 3] ),
    ?assertEqual( 9, real_size() ),
    ?assertEqual([1, 2, 3], out()),
    ?assertEqual( 6, real_size() ),
    ?assertEqual([4, 5, 6], out()),
    ?assertEqual( 3, real_size() ),
    ?assertEqual([7, 8, 9], out()),
    ?assertEqual( 0, real_size() ),
    ?assertEqual([ ], out()),
    ok.


in_3_test() ->
    new(2),
    in(1, 3),
    ?assertEqual({1,1,[1]}, do_get_h_e_b(1)),

    in(2, 3),
    in(3, 3),
    ?assertEqual({1,1,[3, 2, 1]}, do_get_h_e_b(1)),

    in(4, 3),
    ?assertEqual({1,2,[4]}, do_get_h_e_b(2)),
    in(5, 3),
    in(6, 3),
    ?assertEqual({1,2,[6, 5, 4]}, do_get_h_e_b(2)),
    io:format("=========~p ~n",[in(7, 3)]),
    ?assertEqual(false, in(7, 3)),
    ok.

in_2_test() ->
    new(),
    in( 1 ),
    in( 2 ),
    ?assertEqual( do_get_header(), 1),
    ?assertEqual( do_get_ender(), 1),
    ?assertEqual( do_get_block(1), [2, 1]),
    
    new(),
    L0 = lists:seq(1, 1000),
    L1 = lists:reverse( L0 ),
    Fun = fun(X) ->
        in( X )
    end,
    lists:foreach( Fun, L0),

    in(10001),
    in(10002),
    ?assertEqual( do_get_header(), 1),
    ?assertEqual( do_get_ender(), 2),
    ?assertEqual( do_get_block(1), L1),
    ?assertEqual( do_get_block(2), [10002, 10001]),
    
    new(1),
    Fun1 = fun( X ) ->
        in( X )
    end,
    lists:foreach(Fun1, L0),
    ?assertEqual(false, in( 10 )),
    ?assertEqual(do_get_ender(), 1),
    ?assertEqual( true, full() ),
    ok.

full_test() ->
    do_set_header( 1 ),
    MaxSlot = 10,
    do_set_ender( 10 ),
    do_set_max_slot( MaxSlot ),
    do_set_block( MaxSlot, lists:seq( 1, ?MAX_BLOCK_SIZE) ),
    ?assertEqual( full(), true),
    
    do_set_ender(3),
    do_set_header(4),
    do_set_block( 3, lists:seq(1, ?MAX_BLOCK_SIZE )),
    ?assertEqual( full(), true),    

    ok.

empty_test() ->
    do_set_header( 1 ),
    do_set_ender( 1 ),
    do_set_block( 1, [] ),
    ?assertEqual( empty(), true ),

    do_set_header( 10 ),
    do_set_ender( 10 ),
    do_set_block( 10, [] ),
    ?assertEqual( empty(), true ),
    
    
    do_set_block( 10, [1] ),
    ?assertEqual( empty(), false ),
    ok.




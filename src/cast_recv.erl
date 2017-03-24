

-module(cast_recv).

-export([ cast/1 ]).

cast( ZipBin ) ->
    MsgList = erlang:binary_to_term(zlib:unzip( ZipBin )),
    Fun = fun({M, F, A}, _Acc) ->
        erlang:apply(M, F, A)
    end,
    spawn ( fun() -> lists:foldl(Fun, 0, MsgList) end).





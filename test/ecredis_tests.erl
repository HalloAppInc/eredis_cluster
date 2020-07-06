-module(ecredis_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> ecredis_sup:start_link() end).
-define(Clearnup, fun(_) -> ecredis_sup:stop() end).

basic_test_() ->
    {inorder,
        {setup, ?Setup, ?Clearnup,
        [
            { "get and set a",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, ecredis:q(ecredis_a, ["SET", "key", "value"])),
                ?assertEqual({ok, <<"value">>}, ecredis:q(ecredis_a, ["GET","key"])),
                ?assertEqual({ok, undefined}, ecredis:q(ecredis_a, ["GET","nonexists"]))
            end
            },

            { "get and set b",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, ecredis:q(ecredis_b, ["SET", "key", "value"])),
                ?assertEqual({ok, <<"value">>}, ecredis:q(ecredis_b, ["GET","key"])),
                ?assertEqual({ok, undefined}, ecredis:q(ecredis_b, ["GET","nonexists"]))
            end
            },

            { "binary a",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, ecredis:q(ecredis_a, [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
                ?assertEqual({ok, <<"value_binary">>}, ecredis:q(ecredis_a, [<<"GET">>,<<"key_binary">>])),
                ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], ecredis:qp(ecredis_a, [[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]]))
            end
            },

            { "binary b",
            fun() ->
                ?assertEqual({ok, <<"OK">>}, ecredis:q(ecredis_b, [<<"SET">>, <<"key_binary">>, <<"value_binary">>])),
                ?assertEqual({ok, <<"value_binary">>}, ecredis:q(ecredis_b, [<<"GET">>,<<"key_binary">>])),
                ?assertEqual([{ok, <<"value_binary">>},{ok, <<"value_binary">>}], ecredis:qp(ecredis_b, [[<<"GET">>,<<"key_binary">>],[<<"GET">>,<<"key_binary">>]]))
            end
            },

            { "delete test a",
            fun() ->
                ?assertMatch({ok, _}, ecredis:q(ecredis_a, ["DEL", "a"])),
                ?assertEqual({ok, <<"OK">>}, ecredis:q(ecredis_a, ["SET", "b", "a"])),
                ?assertEqual({ok, <<"1">>}, ecredis:q(ecredis_a, ["DEL", "b"])),
                ?assertEqual({ok, undefined}, ecredis:q(ecredis_a, ["GET", "b"]))
            end
            },

            { "delete test b",
            fun() ->
                ?assertMatch({ok, _}, ecredis:q(ecredis_b, ["DEL", "a"])),
                ?assertEqual({ok, <<"OK">>}, ecredis:q(ecredis_b, ["SET", "b", "a"])),
                ?assertEqual({ok, <<"1">>}, ecredis:q(ecredis_b, ["DEL", "b"])),
                ?assertEqual({ok, undefined}, ecredis:q(ecredis_b, ["GET", "b"]))
            end
            },

            { "pipeline a",
            fun () ->
                ?assertNotMatch([{ok, _},{ok, _},{ok, _}], ecredis:qp(ecredis_a, [["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
                ?assertMatch([{ok, _},{ok, _},{ok, _}], ecredis:qp(ecredis_a, [["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]]))
            end
            },

            { "pipeline b",
            fun () ->
                ?assertNotMatch([{ok, _},{ok, _},{ok, _}], ecredis:qp(ecredis_b, [["SET", "a1", "aaa"], ["SET", "a2", "aaa"], ["SET", "a3", "aaa"]])),
                ?assertMatch([{ok, _},{ok, _},{ok, _}], ecredis:qp(ecredis_b, [["LPUSH", "a", "aaa"], ["LPUSH", "a", "bbb"], ["LPUSH", "a", "ccc"]]))
            end
            },

            { "multi node a",
                fun () ->
                    N=1000,
                    Keys = [integer_to_list(I) || I <- lists:seq(1,N)],
                    [ecredis:q(ecredis_a, ["SETEX", Key, "50", Key]) || Key <- Keys],
                    _ = [{ok, integer_to_binary(list_to_integer(Key)+1)} || Key <- Keys],
                    %% ?assertMatch(Guard1, eredis_cluster:qmn([["INCR", Key] || Key <- Keys])),
                    ecredis:q(ecredis_a, ["SETEX", "a", "50", "0"]),
                    _ = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1,5)] %% ,
                    %% ?assertMatch(Guard2, eredis_cluster:qmn([["INCR", "a"] || _I <- lists:seq(1,5)]))
                end
            },

            { "multi node b",
                fun () ->
                    N=1000,
                    Keys = [integer_to_list(I) || I <- lists:seq(1,N)],
                    [ecredis:q(ecredis_b, ["SETEX", Key, "50", Key]) || Key <- Keys],
                    _ = [{ok, integer_to_binary(list_to_integer(Key)+1)} || Key <- Keys],
                    %% ?assertMatch(Guard1, eredis_cluster:qmn([["INCR", Key] || Key <- Keys])),
                    ecredis:q(ecredis_b, ["SETEX", "a", "50", "0"]),
                    _ = [{ok, integer_to_binary(Key)} || Key <- lists:seq(1,5)] %% ,
                    %% ?assertMatch(Guard2, eredis_cluster:qmn([["INCR", "a"] || _I <- lists:seq(1,5)]))
                end
            },

%%            { "transaction",
%%            fun () ->
%%                ?assertMatch({ok,[_,_,_]}, eredis_cluster:transaction([["get","abc"],["get","abc"],["get","abc"]])),
%%                ?assertMatch({error,_}, eredis_cluster:transaction([["get","abc"],["get","abcde"],["get","abcd1"]]))
%%            end
%%            },
%%
%%            { "function transaction",
%%            fun () ->
%%                eredis_cluster:q(["SET", "efg", "12"]),
%%                Function = fun(Worker) ->
%%                    eredis_cluster:qw(Worker, ["WATCH", "efg"]),
%%                    {ok, Result} = eredis_cluster:qw(Worker, ["GET", "efg"]),
%%                    NewValue = binary_to_integer(Result) + 1,
%%                    timer:sleep(100),
%%                    lists:last(eredis_cluster:qw(Worker, [["MULTI"],["SET", "efg", NewValue],["EXEC"]]))
%%                end,
%%                PResult = rpc:pmap({eredis_cluster, transaction},["efg"],lists:duplicate(5, Function)),
%%                Nfailed = lists:foldr(fun({_, Result}, Acc) -> if Result == undefined -> Acc + 1; true -> Acc end end, 0, PResult),
%%                ?assertEqual(4, Nfailed)
%%            end
%%            },

            { "eval key a",
            fun () ->
                ecredis:q(ecredis_a, ["del", "foo"]),
                ecredis:q(ecredis_a, ["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
                ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_a, ["GET", "foo"]))
            end
            },

            { "eval key b",
            fun () ->
                ecredis:q(ecredis_b, ["del", "foo"]),
                ecredis:q(ecredis_b, ["eval","return redis.call('set',KEYS[1],'bar')", "1", "foo"]),
                ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_b, ["GET", "foo"]))
            end
            },

            { "evalsha a",
            fun () ->
                % In this test the key "load" will be used because the "script
                % load" command will be executed in the redis server containing
                % the "load" key. The script should be propagated to other redis
                % client but for some reason it is not done on Travis test
                % environment. @TODO : fix travis redis cluster configuration,
                % or give the possibility to run a command on an arbitrary
                % redis server (no slot derived from key name)
                ecredis:q(ecredis_a, ["del", "load"]),
                {ok, Hash} = ecredis:q(ecredis_a, ["script","load","return redis.call('set',KEYS[1],'bar')"]),
                ecredis:q(ecredis_a, ["evalsha", Hash, 1, "load"]),
                ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_a, ["GET", "load"]))
            end
            },

            { "evalsha b",
            fun () ->
                % In this test the key "load" will be used because the "script
                % load" command will be executed in the redis server containing
                % the "load" key. The script should be propagated to other redis
                % client but for some reason it is not done on Travis test
                % environment. @TODO : fix travis redis cluster configuration,
                % or give the possibility to run a command on an arbitrary
                % redis server (no slot derived from key name)
                ecredis:q(ecredis_b, ["del", "load"]),
                {ok, Hash} = ecredis:q(ecredis_b, ["script","load","return redis.call('set',KEYS[1],'bar')"]),
                ecredis:q(ecredis_b, ["evalsha", Hash, 1, "load"]),
                ?assertEqual({ok, <<"bar">>}, ecredis:q(ecredis_b, ["GET", "load"]))
            end
            },

            { "bitstring support a",
            fun () ->
                ecredis:q(ecredis_a, [<<"set">>, <<"bitstring">>,<<"support">>]),
                ?assertEqual({ok, <<"support">>}, ecredis:q(ecredis_a, [<<"GET">>, <<"bitstring">>]))
            end
            },

            { "bitstring support b",
            fun () ->
                ecredis:q(ecredis_b, [<<"set">>, <<"bitstring">>,<<"support">>]),
                ?assertEqual({ok, <<"support">>}, ecredis:q(ecredis_b, [<<"GET">>, <<"bitstring">>]))
            end
            } %% ,

%%            { "flushdb",
%%            fun () ->
%%                eredis_cluster:q(["set", "zyx", "test"]),
%%                eredis_cluster:q(["set", "zyxw", "test"]),
%%                eredis_cluster:q(["set", "zyxwv", "test"]),
%%                eredis_cluster:flushdb(),
%%                ?assertEqual({ok, undefined}, eredis_cluster:q(["GET", "zyx"])),
%%                ?assertEqual({ok, undefined}, eredis_cluster:q(["GET", "zyxw"])),
%%                ?assertEqual({ok, undefined}, eredis_cluster:q(["GET", "zyxwv"]))
%%            end
%%            },
%%
%%            { "atomic get set",
%%            fun () ->
%%                eredis_cluster:q(["set", "hij", 2]),
%%                Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
%%                Result = rpc:pmap({eredis_cluster, update_key}, [Incr], lists:duplicate(5, "hij")),
%%                IntermediateValues = proplists:get_all_values(ok, Result),
%%                ?assertEqual([3,4,5,6,7], lists:sort(IntermediateValues)),
%%                ?assertEqual({ok, <<"7">>}, eredis_cluster:q(["get", "hij"]))
%%            end
%%            },
%%
%%            { "atomic hget hset",
%%            fun () ->
%%                eredis_cluster:q(["hset", "klm", "nop", 2]),
%%                Incr = fun(Var) -> binary_to_integer(Var) + 1 end,
%%                Result = rpc:pmap({eredis_cluster, update_hash_field}, ["nop", Incr], lists:duplicate(5, "klm")),
%%                IntermediateValues = proplists:get_all_values(ok, Result),
%%                ?assertEqual([{<<"0">>,3},{<<"0">>,4},{<<"0">>,5},{<<"0">>,6},{<<"0">>,7}], lists:sort(IntermediateValues)),
%%                ?assertEqual({ok, <<"7">>}, eredis_cluster:q(["hget", "klm", "nop"]))
%%            end
%%            },
%%
%%            { "eval",
%%            fun () ->
%%                Script = <<"return redis.call('set', KEYS[1], ARGV[1]);">>,
%%                ScriptHash = << << if N >= 10 -> N -10 + $a; true -> N + $0 end >> || <<N:4>> <= crypto:hash(sha, Script) >>,
%%                eredis_cluster:eval(Script, ScriptHash, ["qrs"], ["evaltest"]),
%%                ?assertEqual({ok, <<"evaltest">>}, eredis_cluster:q(["get", "qrs"]))
%%            end
%%            }

      ]
    }
}.

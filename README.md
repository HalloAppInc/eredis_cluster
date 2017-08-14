# eredis_cluster_client

## Description

eredis_cluster_client provides additional functionality to connect multiple redis clusters in your application. 

## TODO

- Improve test suite to demonstrate the case where redis cluster is crashing,
resharding, recovering.
- Add existing eredis_cluster query functions to eredis_cluster_client via gen server calls.


## Compilation && Test

The directory contains a Makefile and rebar3

	make
	make test

## eredis_cluster_client Configuration
To connect multiple redis clusters in your application, you need to start the eredis_cluster_pool supervisor, and multiple  eredis_cluster_client s in your application supervisor as,

    Procs = [{eredis_cluster_pool,
            {eredis_cluster_pool, start_link, [{10, 0}]},
            permanent, 5000, supervisor, [dynamic]},
         {eredis_cluster_client_a,
                {eredis_cluster_client, start_link, [{eredis_cluster_client_a, [{"127.0.0.1",10000},{"127.0.0.1",10001},{"127.0.0.1",10002},{"127.0.0.1",10003},{"127.0.0.1",10004},{"127.0.0.1",10005}]}]},
                permanent, 5000, worker, [dynamic]},
        {eredis_cluster_client_b_1,
            {eredis_cluster_client, start_link, [{eredis_cluster_client_b_1, [{"127.0.0.1",9000},{"127.0.0.1",9001},{"127.0.0.1",9002},{"127.0.0.1",9003},{"127.0.0.1",9004},{"127.0.0.1",9005}]}]},
            permanent, 5000, worker, [dynamic]},
        {eredis_cluster_client_b_2,
            {eredis_cluster_client, start_link, [{eredis_cluster_client_b_2, [{"127.0.0.1",9000},{"127.0.0.1",9001},{"127.0.0.1",9002},{"127.0.0.1",9003},{"127.0.0.1",9004},{"127.0.0.1",9005}]}]},
            permanent, 5000, worker, [dynamic]}
        ],
    {ok, {{one_for_one, 1, 5}, Procs}}.

## Usage
```erlang
%% Simple command
gen_server:call(eredis_cluster_client_a,{q, ["GET","abc"]}).

%% Pipeline
gen_server:call(eredis_cluster_client_a,{qp, [["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]}).

%% Execute a query on the server containing the key "TEST"
gen_server:call(eredis_cluster_client_a,{qk, ["FLUSHDB"], "TEST"}).

%% Flush DB
gen_server:call(eredis_cluster_client_a,{flushdb, ["FLUSHDB"], "TEST"}).

```
## eredis_cluster Configuration

In addition to connect multiple clusters, you can use the eredis_cluster_client as eredis_cluster if you do not need to connect multiple clusters but you wish to integrate it in future.

To configure the redis cluster, you can use an application variable (probably in your app.config):

	{eredis_cluster,
	    [
	        {init_nodes,[
	            {"127.0.0.1",30001},
	            {"127.0.0.1",30002}
	        ]},
	        {pool_size, 5},
	        {pool_max_overflow, 0}
	    ]
	}

You don't need to specify all nodes of your configuration as eredis_cluster will
retrieve them through the command `CLUSTER SLOTS` at runtime.

## Usage

```erlang
%% Start the application
eredis_cluster:start().

%% Simple command
eredis_cluster:q(["GET","abc"]).

%% Pipeline
eredis_cluster:qp([["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

%% Pipeline in multiple node (keys are sorted by node, a pipeline request is
%% made on each node, then the result is aggregated and returned. The response
%% keep the command order
eredis_cluster:qmn([["GET", "a"], ["GET", "b"], ["GET", "c"]]).

%% Transaction
eredis_cluster:transaction([["LPUSH", "a", "a"], ["LPUSH", "a", "b"], ["LPUSH", "a", "c"]]).

%% Transaction Function
Function = fun(Worker) ->
    eredis_cluster:qw(Worker, ["WATCH", "abc"]),
    {ok, Var} = eredis_cluster:qw(Worker, ["GET", "abc"]),

    %% Do something with Var %%
    Var2 = binary_to_integer(Var) + 1,

    {ok, Result} eredis_cluster:qw(Worker,[["MULTI"], ["SET", "abc", Var2], ["EXEC"]]),
    lists:last(Result)
end,
eredis_cluster:transaction(Function, "abc").

%% Optimistic Locking Transaction
Function = fun(GetResult) ->
    {ok, Var} = GetResult,
    Var2 = binary_to_integer(Var) + 1,
    {[["SET", Key, Var2]], Var2}
end,
Result = optimistic_locking_transaction(Key, ["GET", Key], Function),
{ok, {TransactionResult, CustomVar}} = Result

%% Atomic Key update
Fun = fun(Var) -> binary_to_integer(Var) + 1 end,
eredis_cluster:update_key("abc", Fun).

%% Atomic Field update
Fun = fun(Var) -> binary_to_integer(Var) + 1 end,
eredis_cluster:update_hash_field("abc", "efg", Fun).

%% Eval script, both script and hash are necessary to execute the command,
%% the script hash should be precomputed at compile time otherwise, it will
%% execute it at each request. Could be solved by using a macro though.  
Script = "return redis.call('set', KEYS[1], ARGV[1]);",
ScriptHash = "4bf5e0d8612687699341ea7db19218e83f77b7cf",
eredis_cluster:eval(Script, ScriptHash, ["abc"], ["123"]).

%% Flush DB
eredis_cluster:flushdb().

%% Query on all cluster server
eredis_cluster:qa(["FLUSHDB"]).

%% Execute a query on the server containing the key "TEST"
eredis_cluster:qk(["FLUSHDB"], "TEST").
```

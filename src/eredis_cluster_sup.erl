-module(eredis_cluster_sup).
-behaviour(supervisor).

%% Supervisor.
-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
	-> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    Procs = [{eredis_cluster_pool,
        {eredis_cluster_pool, start_link, [{25,0}]},
        permanent, 5000, supervisor, [dynamic]},
        {eredis_cluster_client_c,
            {eredis_cluster_client, start_link, [{eredis_cluster_client_c, [{"127.0.0.1",9000},{"127.0.0.1",9001},{"127.0.0.1",9002},{"127.0.0.1",9003},{"127.0.0.1",9004},{"127.0.0.1",9005}]}]},
            permanent, 5000, worker, [dynamic]},
        {eredis_cluster_client_d,
            {eredis_cluster_client, start_link, [{eredis_cluster_client_d, [{"127.0.0.1",9000},{"127.0.0.1",9001},{"127.0.0.1",9002},{"127.0.0.1",9003},{"127.0.0.1",9004},{"127.0.0.1",9005}]}]},
            permanent, 5000, worker, [dynamic]}
    ],
    {ok, {{one_for_one, 1, 5}, Procs}}.

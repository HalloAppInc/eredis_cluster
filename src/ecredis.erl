-module(ecredis).
-behaviour(gen_server).

%% API.
-export([start_link/1,
         remap_cluster/2,
         get_eredis_pid/2,
         qp/2,
         q/2]).

%% Callbacks for gen_server.
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% Type definition.
-include_lib("eunit/include/eunit.hrl").
-include("eredis_cluster.hrl").
-record(state, {
    cluster_name :: string(),
    init_nodes :: [#node{}],
    slots_cache :: tuple(), %% Tuple with all the Redis slots. Each element indexes into slots_maps.
    slots_maps :: tuple(), %% Tuple with all the Redis nodes and Redis slots mapped on them.
    version :: integer()  %% Used to avoid unnecessary refresh of Redis slots.
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link([]) -> {ok, pid()}.
start_link({ClusterName, Args}) ->
    gen_server:start_link({local, ClusterName}, ?MODULE, [ClusterName, Args], []).


%% Looks up eredis Pid for Slot.
-spec get_eredis_pid(ClusterName::atom(), Slot::integer()) -> 
    {Pid::pid(), Version::integer()} | undefined.
get_eredis_pid(ClusterName, Slot) ->
    Result = ets:lookup(ets_table_name(ClusterName), Slot),
    case Result of
        [] -> undefined;
        [{_, Result2}] -> Result2
    end. 


%% Remaps various redis slots.
-spec remap_cluster(ClusterName::atom(), Version::integer()) -> {Version::integer()}.
remap_cluster(ClusterName, Version) ->
    Result = gen_server:call(ClusterName, {remap_cluster, Version}),
    Result.


%% Executes qp.
-spec qp(ClusterName::atom(), redis_pipeline_command()) -> redis_pipeline_result().
qp(ClusterName, Commands) ->
    q(ClusterName, Commands).


%% Executes q.
-spec q(ClusterName::atom(), redis_command()) -> redis_result().
q(ClusterName, Command) ->
    query(ClusterName, Command).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal methods
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


-spec connect_(State::#state{}, [{Address::string(), Port::integer()}]) -> #state{}.
connect_(_State, []) ->
    #state{};
connect_(State, InitNodes) ->
    NewState = State#state{
        slots_cache = {},
        slots_maps = {},
        init_nodes = [#node{address = A, port = P} || {A,P} <- InitNodes],
        version = 0
    },
    reload_slots_map(NewState).


-spec remap_cluster_internal(State::#state{}, Version::integer()) -> State::#state{}.
remap_cluster_internal(State, Version) ->
    if State#state.version == Version ->
        reload_slots_map(State);
    true ->
        State
    end.


%% TODO(vipin): Need to reset connection on Redis node removal.
-spec reload_slots_map(State::#state{}) -> State::#state{}.
reload_slots_map(State) ->
    ClusterSlots = get_cluster_slots(State#state.cluster_name, State#state.init_nodes),
    SlotsMaps = parse_cluster_slots(ClusterSlots),
    ConnectedSlotsMaps = connect_all_slots(State#state.cluster_name, SlotsMaps),
    SlotsCache = create_slots_cache(ConnectedSlotsMaps),

    NewState = State#state{
        slots_cache = list_to_tuple(SlotsCache),
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        version = State#state.version + 1
    },
    [cache_eredis_pids(NewState, Slot) || Slot <- lists:seq(0, ?REDIS_CLUSTER_HASH_SLOTS - 1)],

    %% Reset tuples not needed in the state.
    NewState2 = NewState#state{
      slots_cache = {},
      slots_maps = {}
    },
    NewState2.

-spec get_cluster_slots(ClusterName::atom(), [#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots(_ClusterName, []) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots(ClusterName, [Node|T]) ->
    Res = lookup_eredis_pid(ClusterName, Node#node.address, Node#node.port),
    case Res of
        {ok, Pid} ->
          case eredis:q(Pid, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                ClusterInfo;
            _ ->
                get_cluster_slots(ClusterName, T)
          end;
        _ ->
            get_cluster_slots(ClusterName, T)
  end.


-spec get_cluster_slots_from_single_node(#node{}) -> [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
    [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS-1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].


-spec parse_cluster_slots([[bitstring() | [bitstring()]]]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo) ->
    parse_cluster_slots(ClusterInfo, 1, []).

parse_cluster_slots([[StartSlot, EndSlot | [[Address, Port | _] | _]] | T], Index, Acc) ->
    SlotsMap =
        #slots_map{
            index = Index,
            start_slot = binary_to_integer(StartSlot),
            end_slot = binary_to_integer(EndSlot),
            node = #node{
                address = binary_to_list(Address),
                port = binary_to_integer(Port)
            }
        },
    parse_cluster_slots(T, Index+1, [SlotsMap | Acc]);
parse_cluster_slots([], _Index, Acc) ->
    lists:reverse(Acc).


-spec connect_all_slots(ClusterName::atom(), [#slots_map{}]) -> [integer()].
connect_all_slots(ClusterName, SlotsMapList) ->
    [SlotsMap#slots_map{node=connect_node(ClusterName, SlotsMap#slots_map.node)} ||
     SlotsMap <- SlotsMapList].


-spec connect_node(ClusterName::atom(), #node{}) -> #node{} | undefined.
connect_node(ClusterName, Node) ->
    Res = lookup_eredis_pid(ClusterName, Node#node.address, Node#node.port),
    case Res of
        {ok, _} -> Node;
        _ -> undefined
    end.


%% Creates tuple with one element per Redis slot. Each element maps the Redis slot to the Redis
%% node index in SlotsMaps.
-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index, SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot, SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  FlatSlotsCache = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(FlatSlotsCache),
  [ Index || {_,Index} <- SortedSlotsCache].


-spec cache_eredis_pids(State::#state{}, Slot::integer()) -> ok.
cache_eredis_pids(State, Slot) ->
    RedisNodeIndex = element(Slot + 1, State#state.slots_cache),
    SlotsMap = element(RedisNodeIndex, State#state.slots_maps),
    NewPid = if
        SlotsMap#slots_map.node =/= undefined ->
            {ok, Pid} = lookup_eredis_pid(State#state.cluster_name,
                                          SlotsMap#slots_map.node#node.address,
                                          SlotsMap#slots_map.node#node.port),
            Pid;
        true ->
            undefined
    end,
    ets:insert(ets_table_name(State#state.cluster_name), {Slot, {NewPid, State#state.version}}),
    ok.


query(ClusterName, Command) ->
    Key = get_key_from_command(Command),
    query(ClusterName, Command, Key).


query(_Cluster, _, undefined) ->
    {error, invalid_cluster_key};
query(ClusterName, Command, Key) ->
    Slot = get_key_slot(Key),
    {Pid, Version} = get_eredis_pid(ClusterName, Slot),
    query(ClusterName, Pid, Command, Slot, Version, 0).


query(_Cluster, undefined, _, _, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {error, no_connection};
query(ClusterName, Pid, Command, Slot, Version, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    case eredis_query(Pid, Command) of
        % If we detect a node went down, we should probably refresh the slot
        % mapping.
        {error, no_connection} ->
            {ok, _} = remap_cluster(ClusterName, Version),
            {NewPid, NewVersion} = get_eredis_pid(ClusterName, Slot), 
            query(ClusterName, NewPid, Command, Slot, NewVersion, Counter + 1);

        % If the tcp connection is closed (connection timeout), the redis worker
        % will try to reconnect, thus the connection should be recovered for
        % the next request. We don't need to refresh the slot mapping in this
        % case
        {error, tcp_closed} ->
            query(ClusterName, Pid, Command, Slot, Version, Counter + 1);

        % Redis explicitly say our slot mapping is incorrect, we need to refresh
        % it
        {error, <<"MOVED ", _/binary>>} ->
            {ok, _} = remap_cluster(ClusterName, Version),
            {NewPid, NewVersion} = get_eredis_pid(ClusterName, Slot), 
            query(ClusterName, NewPid, Command, Slot, NewVersion, Counter + 1);

        Result ->
            Result
    end.


eredis_query(Pid, [[X|_]|_] = Commands) when is_list(X); is_binary(X) ->
    eredis:qp(Pid, Commands);
eredis_query(Pid, Command) ->
    eredis:q(Pid, Command).


-spec throttle_retries(integer()) -> ok.
throttle_retries(0) -> ok;
throttle_retries(_) -> timer:sleep(?REDIS_RETRY_DELAY).


%%% =============================================================================
%% @doc Return the first key in the command arguments.
%% In a normal query, the second term will be returned
%%
%% If it is a pipeline query we will use the second term of the first term, we
%% will assume that all keys are in the same server and the query can be
%% performed
%%
%% If the pipeline query starts with multi (transaction), we will look at the
%% second term of the second command
%%
%% For eval and evalsha command we will look at the fourth term.
%%
%% For commands that don't make sense in the context of cluster
%% return value will be undefined.
%% @end
%% =============================================================================
-spec get_key_from_command(redis_command()) -> string() | undefined.
get_key_from_command([[X|Y]|Z]) when is_bitstring(X) ->
    get_key_from_command([[bitstring_to_list(X)|Y]|Z]);
get_key_from_command([[X|Y]|Z]) when is_list(X) ->
    case string:to_lower(X) of
        "multi" ->
            get_key_from_command(Z);
        _ ->
            get_key_from_command([X|Y])
    end;
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term1) ->
    get_key_from_command([bitstring_to_list(Term1),Term2|Rest]);
get_key_from_command([Term1,Term2|Rest]) when is_bitstring(Term2) ->
    get_key_from_command([Term1,bitstring_to_list(Term2)|Rest]);
get_key_from_command([Term1,Term2|Rest]) ->
    case string:to_lower(Term1) of
        "info" ->
            undefined;
        "config" ->
            undefined;
        "shutdown" ->
            undefined;
        "slaveof" ->
            undefined;
        "eval" ->
            get_key_from_rest(Rest);
        "keys" ->
            keys;
        "evalsha" ->
            get_key_from_rest(Rest);
        _ ->
            Term2
    end;
get_key_from_command(_) ->
    undefined.

%% =============================================================================
%% @doc Get key for command where the key is in th 4th position (eval and
%% evalsha commands)
%% @end
%% =============================================================================
-spec get_key_from_rest([anystring()]) -> string() | undefined.
get_key_from_rest([_,KeyName|_]) when is_bitstring(KeyName) ->
    bitstring_to_list(KeyName);
get_key_from_rest([_,KeyName|_]) when is_list(KeyName) ->
    KeyName;
get_key_from_rest(_) ->
    undefined.

%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================
-spec get_key_slot(Key::any()) -> Slot::integer().
get_key_slot(K) ->
    %% cast any type of key to a list.
    Key = lists:concat([K]),
    KeyToBeHashed = case string:chr(Key,${) of
                        0 ->
                            Key;
                        Start ->
                            case string:chr(string:substr(Key,Start+1),$}) of
                                0 ->
                                    Key;
                                Length ->
                                    if
                                        Length =:= 1 ->
                                            Key;
                                        true ->
                                            string:substr(Key,Start+1,Length-1)
                                    end
                            end
                    end,
    eredis_cluster_hash:hash(KeyToBeHashed).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% To manage list of eredis connections
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_ets_table(ClusterName) ->
    ets:new(ets_table_name(ClusterName), [protected, set, named_table, {read_concurrency, true}]).

%% Looks up existing redis client pid using Ip, Port and return Pid of the client
%% connection. If no such connection is present, this method establishes the connection.
-spec lookup_eredis_pid(ClusterName::atom(), Ip::string(), Port::integer()) -> {ok, Pid::pid()}.
lookup_eredis_pid(ClusterName, Ip, Port) ->
    Res = ets:lookup(ets_table_name(ClusterName), [Ip, Port]),  
    case Res of
        [] ->
           {ok, Pid} = safe_eredis_start_link(Ip, Port),
           ets:insert(ets_table_name(ClusterName), {[Ip, Port], Pid}),
          {ok, Pid};
        [{_, Pid}] -> {ok, Pid}
    end.

ets_table_name(ClusterName) ->
    list_to_atom(atom_to_list(ClusterName) ++ atom_to_list(?MODULE)).


safe_eredis_start_link(Ip, Port) ->
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Ip, Port),
    process_flag(trap_exit, false),
    Payload.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server call backs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init([ClusterName, InitNodes]) ->
    State = #state{
        cluster_name = ClusterName
    },
    create_ets_table(ClusterName),
    {ok, connect_(State, InitNodes)}.


handle_call({remap_cluster, Version}, _From, State) ->
    NewState = remap_cluster_internal(State, Version),
    {reply, NewState#state.version, NewState};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

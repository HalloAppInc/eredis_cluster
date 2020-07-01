-module(eredis_cluster_manager).
-behaviour(gen_server).

%% API.
-export([start_link/1]).
-export([remap_cluster/2]).
-export([get_eredis_pid/2]).
-export([qp/2]).
-export([q/2]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").
-record(state, {
    init_nodes :: [#node{}],
    slots :: tuple(), %% Tuple with all the Redis slots. Each element is index into slots_maps.
    slots_maps :: tuple(), %% Tuple with all the Redis nodes and Redis slots mapped on them.
    version :: integer()  %% Used to avoid unnecessary refresh of Redis slots.
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_link([]) -> {ok, pid()}.
start_link({ModName, Args}) ->
    gen_server:start_link({local, ModName}, ?MODULE, [Args], []).

%% Looks up eredis Pid for Slot.
-spec get_eredis_pid(Cluster::any(), Slot::integer()) -> 
    {Pid::pid() | undefined, Version::integer()}.
get_eredis_pid(Cluster, Slot) ->
    {ok, Result} = gen_server:call(Cluster, {get_pid_by_slot, Slot}),
    Result.


%% Remaps various redis slots.
-spec remap_cluster(Cluster::any(), Version::integer()) -> {Version::integer()}.
remap_cluster(Cluster, Version) ->
    {ok, Version} = gen_server:call(Cluster, {refresh_mapping, Version}),
    Version.


-spec qp(Cluster::any(), redis_pipeline_command()) -> redis_pipeline_result().
qp(Cluster, Commands) ->
    q(Cluster, Commands).


-spec q(Cluster::any(), redis_command()) -> redis_result().
q(Cluster, Command) ->
    query(Cluster ,Command).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Internal methods
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_pid_by_slot(State::#state{}, Slot::integer()) ->
    {Pid::pid() | undefined, Version::integer()}.
get_pid_by_slot(State, Slot) ->
    Index = element(Slot + 1, State#state.slots),
    Cluster = element(Index, State#state.slots_maps),
    if
        Cluster#slots_map.node =/= undefined ->
            {ok, Pid} = lookup_eredis_pid(Cluster#slots_map.node#node.address,
                                          Cluster#slots_map.node#node.port),
            {Pid, State#state.version};
        true ->
            {undefined, State#state.version}
    end.


-spec refresh_mapping(State::#state{}, Version::integer()) -> State::#state{}.
refresh_mapping(State, Version) ->
    if State#state.version == Version ->
        reload_slots_map(State);
    true ->
        State
    end.


%% TODO(vipin): Need to reset connection on Redis node removal.
-spec reload_slots_map(State::#state{}) -> State::#state{}.
reload_slots_map(State) ->
    ClusterSlots = get_cluster_slots(State#state.init_nodes),
    SlotsMaps = parse_cluster_slots(ClusterSlots),
    ConnectedSlotsMaps = connect_all_slots(SlotsMaps),
    Slots = create_slots_cache(ConnectedSlotsMaps),

    NewState = State#state{
        slots = list_to_tuple(Slots),
        slots_maps = list_to_tuple(ConnectedSlotsMaps),
        version = State#state.version + 1
    },
    NewState.

-spec get_cluster_slots([#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots([]) ->
    throw({error,cannot_connect_to_cluster});
get_cluster_slots([Node|T]) ->
    case lookup_eredis_pid(Node#node.address, Node#node.port) of
        {ok, Pid} ->
          case eredis:q(Pid, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                ClusterInfo;
            _ ->
                get_cluster_slots(T)
          end;
        _ ->
            get_cluster_slots(T)
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


%% Creates tuple with one element per Redis slot. Each element maps the Redis slot to the Redis
%% node index in SlotsMaps.
-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index, SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot, SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].


-spec connect_all_slots([#slots_map{}]) -> [integer()].
connect_all_slots(SlotsMapList) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)} || SlotsMap <- SlotsMapList].


-spec connect_node(#node{}) -> #node{} | undefined.
connect_node(Node) ->
    case lookup_eredis_pid(Node#node.address, Node#node.port) of
        {ok, _} -> Node;
        _ -> undefined
    end.


-spec connect_([{Address::string(), Port::integer()}]) -> #state{}.
connect_([]) ->
    #state{};
connect_(InitNodes) ->
    State = #state{
        slots = undefined,
        slots_maps = {},
        init_nodes = [#node{address = A, port = P} || {A,P} <- InitNodes],
        version = 0
    },
    reload_slots_map(State).


query(Cluster, Command) ->
    Key = get_key_from_command(Command),
    query(Cluster, Command, Key).


query(_Cluster, _, undefined) ->
    {error, invalid_cluster_key};
query(Cluster, Command, Key) ->
    Slot = get_key_slot(Key),
    %% TODO(vipin): Need to get pid from ets table instead of gen_server.
    {Pid, Version} = get_eredis_pid(Cluster, Slot),
    query(Cluster, Pid, Command, Slot, Version, 0).

query(_Cluster, undefined, _, _, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {error, no_connection};
query(Cluster, Pid, Command, Slot, Version, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),

    case eredis_query(Pid, Command) of
        % If we detect a node went down, we should probably refresh the slot
        % mapping.
        {error, no_connection} ->
            {ok, _} = remap_cluster(Cluster, Version),
            {NewPid, NewVersion} = get_eredis_pid(Cluster, Slot), 
            query(Cluster, NewPid, Command, Slot, NewVersion, Counter+1);

        % If the tcp connection is closed (connection timeout), the redis worker
        % will try to reconnect, thus the connection should be recovered for
        % the next request. We don't need to refresh the slot mapping in this
        % case
        {error, tcp_closed} ->
            query(Cluster, Pid, Command, Slot, Version, Counter+1);

        % Redis explicitly say our slot mapping is incorrect, we need to refresh
        % it
        {error, <<"MOVED ", _/binary>>} ->
            {ok, _} = remap_cluster(Cluster, Version),
            {NewPid, NewVersion} = get_eredis_pid(Cluster, Slot), 
            query(Cluster, NewPid, Command, Slot, NewVersion, Counter+1);

        Payload ->
            {ok, Payload}
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
% Methods to manage list of eredis connections
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init_eredis_connections() ->
    ets:new(?MODULE, [protected, set, named_table, {read_concurrency, true}]).

%% Looks up existing redis client pid using Ip, Port and return Pid of the client
%% connection. If no such connection is present, this method establishes the connection.
-spec lookup_eredis_pid(Ip::string(), Port::integer()) -> {ok, Pid::pid()}.
lookup_eredis_pid(Ip, Port) ->
    case ets:lookup(?MODULE, [Ip, Port]) of
        [] ->
           {ok, Pid} = safe_eredis_start_link(Ip, Port),
           ets:insert(?MODULE, {[Ip, Port], Pid}),
          {ok, Pid};
        [_, Pid] -> {ok, Pid}
    end.


%% Closes redis client connection if present in cstate.
%%-spec delete(Ip::string(), Port::integer()) -> {ok}.
%%delete(Ip, Port) ->
%%    case ets:lookup(?MODULE, [Ip, Port]) of
%%        [_, Pid] ->
%%             safe_eredis_stop(Pid),
%%             ets:remove(?MODULE, [Ip, Port]),
%%            ok; 
%%        {_, _} -> {ok}
%%    end. 


safe_eredis_start_link(Ip, Port) ->
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Ip, Port),
    process_flag(trap_exit, false),
    Payload.


%%safe_eredis_stop(Pid) ->
%%    process_flag(trap_exit, true),
%%    eredis:stop(Pid),
%%    process_flag(trap_exit, false).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server call backs
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init([Args]) ->
    init_eredis_connections(),
    {ok, connect_(Args)}.


handle_call({get_pid_by_slot, Slot}, _From, State) ->
    {Pid, Version} = get_pid_by_slot(State, Slot),
    {reply, {Pid, Version}, State};
handle_call({refresh_mapping, Version}, _From, State) ->
    NewState = refresh_mapping(State, Version),
    {reply, NewState#state.version, NewState};
handle_call({connect, InitServers}, _From, _State) ->
    {reply, ok, connect_(InitServers)};
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

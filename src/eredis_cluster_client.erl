-module(eredis_cluster_client).
-behaviour(gen_server).

%% API.
-export([start_link/1]).
-export([refresh_mapping/2]).
-export([get_pool_by_slot/2]).
-export([get_all_pools/1]).

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
    slots :: tuple(), %% whose elements are integer indexes into slots_maps
    slots_maps :: tuple(), %% whose elements are #slots_map{}
    version :: integer()
}).

%% API.
-spec start_link([]) -> {ok, pid()}.
start_link({ModName, Args}) ->
    gen_server:start_link({local,ModName}, ?MODULE, [Args], []).

%%connect(InitServers) ->
%%    gen_server:call(?MODULE,{connect,InitServers}).

refresh_mapping(State, Version) ->
    if State#state.version == Version ->
        reload_slots_map(State);
    true ->
        State
    end.

%%%% =============================================================================
%%%% @doc Given a slot return the link (Redis instance) to the mapped
%%%% node.
%%%% @end
%%%% =============================================================================

-spec get_all_pools(State::#state{}) -> [pid()].
get_all_pools(State) ->
    SlotsMapList = tuple_to_list(State#state.slots_maps),
    [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
        SlotsMap#slots_map.node =/= undefined].

-spec get_pool_by_slot(State::#state{}, Slot::integer()) ->
    {PoolName::atom() | undefined, Version::integer()}.
get_pool_by_slot(State,Slot) ->
    Index = element(Slot+1,State#state.slots),
    Cluster = element(Index,State#state.slots_maps),

    if
        Cluster#slots_map.node =/= undefined ->
            {Cluster#slots_map.node#node.pool, State#state.version};
        true ->
            {undefined, State#state.version}
    end.

-spec reload_slots_map(State::#state{}) -> NewState::#state{}.
reload_slots_map(State) ->

    [close_connection(SlotsMap)
        || SlotsMap <- tuple_to_list(State#state.slots_maps)],

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
    case safe_eredis_start_link(Node#node.address, Node#node.port) of
        {ok,Connection} ->
          case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
            {error,<<"ERR unknown command 'CLUSTER'">>} ->
                get_cluster_slots_from_single_node(Node);
            {error,<<"ERR This instance has cluster support disabled">>} ->
                get_cluster_slots_from_single_node(Node);
            {ok, ClusterInfo} ->
                eredis:stop(Connection),
                ClusterInfo;
            _ ->
                eredis:stop(Connection),
                get_cluster_slots(T)
        end;
        _ ->
            get_cluster_slots(T)
  end.

-spec get_cluster_slots_from_single_node(#node{}) ->
    [[bitstring() | [bitstring()]]].
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

-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
    Node = SlotsMap#slots_map.node,
    if
        Node =/= undefined ->
            try eredis_cluster_pool:stop(Node#node.pool) of
                _ ->
                    ok
            catch
                _ ->
                    ok
            end;
        true ->
            ok
    end.

-spec connect_node(#node{}) -> #node{} | undefined.
connect_node(Node) ->
    case eredis_cluster_pool:create(Node#node.address, Node#node.port) of
        {ok, Pool} ->
            Node#node{pool=Pool};
        _ ->
            undefined
    end.

safe_eredis_start_link(Address,Port) ->
    process_flag(trap_exit, true),
    Payload = eredis:start_link(Address, Port),
    process_flag(trap_exit, false),
    Payload.

-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index,SlotsMap#slots_map.index}
        || Index <- lists:seq(SlotsMap#slots_map.start_slot,
            SlotsMap#slots_map.end_slot)]
        || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [ Index || {_,Index} <- SortedSlotsCache].

-spec connect_all_slots([#slots_map{}]) -> [integer()].
connect_all_slots(SlotsMapList) ->
    [SlotsMap#slots_map{node=connect_node(SlotsMap#slots_map.node)}
        || SlotsMap <- SlotsMapList].

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


init([Args]) ->
    {ok, connect_(Args)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% eredis_cluster_client functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% =============================================================================
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
%% @doc Wrapper function for command using pipelined commands
%% @end
%% =============================================================================
-spec qp(State::#state{}, redis_pipeline_command()) -> redis_pipeline_result().
qp(State, Commands) -> q(State,Commands).
%% =============================================================================
%% @doc This function execute simple or pipelined command on a single redis node
%% the node will be automatically found according to the key used in the command
%% @end
%% =============================================================================
-spec q(State::#state{}, redis_command()) -> redis_result().
q(State, Command) ->
    query(State ,Command).

query(State, Command) ->
    PoolKey = get_key_from_command(Command),
    query(State, Command, PoolKey).

query(State, Command , keys) ->
    KeysFromAll = qa(State,Command),
    Resp = murge_multi_query_response(KeysFromAll),
    {State, ok, Resp};

query(State, _, undefined) ->
    {State, error, invalid_cluster_command};
query(State, Command, PoolKey) ->
    Slot = get_key_slot(PoolKey),

    Transaction = fun(Worker) -> qw(Worker, Command) end,
    query(State, Transaction, Slot, 0).

query(State, _, _, ?REDIS_CLUSTER_REQUEST_TTL) ->
    {State, error, no_connection};
query(State, Transaction, Slot, Counter) ->
    %% Throttle retries
    throttle_retries(Counter),
    {Pool, Version} = get_pool_by_slot(State, Slot),

    case eredis_cluster_pool:transaction(Pool, Transaction) of
        % If we detect a node went down, we should probably refresh the slot
        % mapping.
        {error, no_connection} ->
            NewState = refresh_mapping(State, Version),
            query(NewState, Transaction, Slot, Counter+1);

        % If the tcp connection is closed (connection timeout), the redis worker
        % will try to reconnect, thus the connection should be recovered for
        % the next request. We don't need to refresh the slot mapping in this
        % case
        {error, tcp_closed} ->
            query(State, Transaction, Slot, Counter+1);

        % Redis explicitly say our slot mapping is incorrect, we need to refresh
        % it
        {error, <<"MOVED ", _/binary>>} ->
            NewState = refresh_mapping(State, Version),
            query(NewState, Transaction, Slot,  Counter+1);

        Payload ->
            {State, ok, Payload}
    end.

murge_multi_query_response(Response)->
    murge_multi_query_response(Response,[]).
murge_multi_query_response([], MurgeResp)->
    MurgeResp;
murge_multi_query_response([Head|Rest], MurgeResp)->
    io:format("Multi Query :~p ~n",[Head]),
    case Head of
        {ok,Resp} ->
            Murged = MurgeResp ++ Resp;
        _Error ->
            Murged = MurgeResp
    end,
    murge_multi_query_response(Rest,Murged).

-spec throttle_retries(integer()) -> ok.
throttle_retries(0) -> ok;
throttle_retries(_) -> timer:sleep(?REDIS_RETRY_DELAY).
%% =============================================================================
%% @doc Return the hash slot from the key
%% @end
%% =============================================================================
-spec get_key_slot(Key::any()) -> Slot::integer().
%%get_key_slot(Key) when is_bitstring(Key) ->
%%    get_key_slot(bitstring_to_list(Key));
%%get_key_slot(Key) when is_integer(Key) ->
%%    get_key_slot(integer_to_list(Key));
get_key_slot(K) ->
%%    cast any type of key to a list.
    Key = lists:concat([K]),
    KeyToBeHased = case string:chr(Key,${) of
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
    eredis_cluster_hash:hash(KeyToBeHased).

%% =============================================================================
%% @doc Perform a given query on all node of a redis cluster
%% @end
%% =============================================================================
-spec qa(State::#state{}, redis_command()) -> ok | {error, Reason::bitstring()}.
qa(State, Command) ->
    Pools = get_all_pools(State),
    Transaction = fun(Worker) -> qw(Worker, Command) end,
    [eredis_cluster_pool:transaction(Pool, Transaction) || Pool <- Pools].


-spec qs(Pool :: atom(), redis_command()) -> redis_result().
qs(Pool, Command) ->
    Transaction = fun(Worker) -> qw(Worker, Command) end,
    eredis_cluster_pool:transaction(Pool, Transaction).

%% =============================================================================
%% @doc Wrapper function to be used for direct call to a pool worker in the
%% function passed to the transaction/2 method
%% @end
%% =============================================================================
-spec qw(Worker::pid(), redis_command()) -> redis_result().
qw(Worker, Command) ->
    eredis_cluster_pool_worker:query(Worker, Command).

%% =============================================================================
%% @doc Perform flushdb command on each node of the redis cluster
%% @end
%% =============================================================================
-spec flushdb(State::#state{}) -> ok | {error, Reason::bitstring()}.
flushdb(State) ->
    Result = qa(State,["FLUSHDB"]),
    case proplists:lookup(error,Result) of
        none ->
            ok;
        Error ->
            Error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% gen_server call backs - Redis exposed query functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call({q,Command}, _From, State) ->
    {NewState, Status, Resp} = q(State, Command),
    {reply, {Status,Resp}, NewState};

handle_call({qp,Commands}, _From, State) ->
    {NewState, Status, Resp} = qp(State, Commands),
    {reply, {Status,Resp}, NewState};

handle_call({qk,Command, PoolKey}, _From, State) ->
    {NewState, Status, Resp} = query(State, Command, PoolKey),
    {reply, {Status,Resp}, NewState};

handle_call({qa, Command}, _From, State) ->
    Resp = qa(State, Command),
    {reply, {ok, Resp}, State};

handle_call({qs, Pool, Command}, _From, State) ->
    Resp = qs(Pool, Command),
    {reply, Resp, State};

handle_call({qw, Worker, Command}, _From, State) ->
    Resp = qw(Worker, Command),
    {reply, Resp, State};

handle_call({get_all_pools}, _From, State) ->
    Pools = get_all_pools(State),
    {reply, {ok, Pools}, State};

handle_call({connect, InitServers}, _From, _State) ->
    {reply, ok, connect_(InitServers)};

handle_call(flushdb, _From, State) ->
    Result = flushdb(State),
    {reply, {ok, Result}, State};

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(flushdb, State) ->
    flushdb(State),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

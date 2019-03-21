%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(emqx_bridge_mqtt_worker).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-import(proplists, [get_value/2]).


-export([start_link/0, stop/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {mqttc, host, port, username, password, keepalive, topics, subqos,
                landing_enable, landing_topic, landing_maxqos}).

-define(APP, emqx_bridge_mqtt).

-define(CLIENT_ID, <<"emqx_bridge_mqtt">>).

-define(LOG(Level, Fmt, Args), lager:Level("emqx_bridge_mqtt_worker " ++ Fmt, Args)).

%%------------------------------------------------------------------------------
%% API Function
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

%%------------------------------------------------------------------------------
%% gen_server Callbacks
%%------------------------------------------------------------------------------

init(_Args) ->
    %% Trace the exit single
    process_flag(trap_exit, true),

    ServerCfg = application:get_env(?APP, server, []),
    Host = get_value(host, ServerCfg),
    Port = get_value(port, ServerCfg),
    Username = get_value(username, ServerCfg),
    Password = get_value(password, ServerCfg),
    Keepalive = get_value(keepalive, ServerCfg),

    RemoteCfg = application:get_env(?APP, remote, []),
    Topics = get_value(topics, RemoteCfg),
    SubQos = get_value(subqos, RemoteCfg),

    LandingCfg = application:get_env(?APP, landing, []),
    LandingEnable = get_value(enable, LandingCfg),
    LandingMaxQos = get_value(maxqos, LandingCfg),
    LandingTopic = get_value(topic, LandingCfg),

    %% Preper to connect the remote server
    self() ! connect,

    ?LOG(info, "initialed with ~s:~p~n", [Host, Port]),

    {ok, #state{host = Host, port = Port, username = Username, password = Password,
                keepalive = Keepalive, topics = Topics, subqos = SubQos, landing_enable = LandingEnable,
                landing_topic = LandingTopic, landing_maxqos = LandingMaxQos}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, #state{mqttc = OldClient,
                            host = Host,
                            port = Port,
                            username = Username,
                            password = Password,
                            keepalive = Keepalive} = State) ->
    ?LOG(debug, "will connect to ~s:~p~n", [Host, Port]),
    case OldClient of 
        undefined -> ignore;
        OldClient -> emqttc:disconnect(OldClient)
    end, 

    {ok, C} = emqttc:start_link([{host, Host},
                                 {port, Port},
                                 {client_id, ?CLIENT_ID},
                                 {username, Username},
                                 {password, Password},
                                 {keepalive, Keepalive},
                                 {reconnect, 1},            %% Reconect interval => (if failed will double it)
                                 {logger, {console, info}}]),
    {noreply, State#state{mqttc = C}};

%% Receive Messages
handle_info({publish, Topic, Payload}, State) ->
    ?LOG(debug, "received from ~p, payload ~p~n", [Topic, Payload]),
    forward(Topic, Payload, State),
    {noreply, State};

%% Client connected
handle_info({mqttc, C, connected}, State = #state{mqttc = C, host = Host, port = Port,
                                                  topics = Topics, subqos = SubQos}) ->
    ?LOG(info, "mqttc(~p) has connected to ~s:~p~n", [C, Host, Port]),

    %% Subscribe topics, when the mqttc connected to remote server
    lists:foreach(fun(T) ->
                    emqttc:subscribe(C, T, SubQos)
                  end, Topics),

    {noreply, State};

%% Client disconnected
handle_info({mqttc, C,  disconnected}, State = #state{mqttc = C}) ->
    ?LOG(warning, "mqttc client ~p has disconnected~n", [C]),

    %% XXX: after a interval reconnect to the remote
    %% The reconect timer
    erlang:send_after(100, self(), connect),

    {noreply, State};

handle_info(Info, State) ->
    ?LOG(warning, "UNEXCEPTED MSG ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

forward(OriginalTopic, Payload, #state{landing_enable = LandingEnable,
                                       landing_maxqos = MaxQos,
                                       landing_topic  = LandingTopic}) ->
    Topic = case LandingEnable of
                true -> LandingTopic;
                false -> OriginalTopic
            end,
    Headers = [{original_topic, OriginalTopic}],
    Msg = emqx_message:make(<<"emqx_bridge_mqtt_worker">>, MaxQos, Topic, Payload, Headers),
    case emqx:publish(Msg) of
        {ok, _Delivery} ->
            ?LOG(debug,"delivered the message ~p~n", [Payload]);
        _ ->
            ?LOG(warning, "delivered the ~p success, but not recevier~n", [Payload])
    end, ok.


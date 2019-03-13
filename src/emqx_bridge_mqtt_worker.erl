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

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, stop/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-import(proplists, [get_value/2]).

-record(state, {mqttc, host, port, username, password, keepalive, topics, subqos,
                landing_topic, landing_maxqos}).

-define(APP, emqx_bridge_mqtt).
-define(CLIENT_ID, <<"emqx_bridge_mqtt">>).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:call(?SERVER, stop).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    ServerCfg = application:get_env(?APP, server, []),
    Host = get_value(host, ServerCfg),
    Port = get_value(port, ServerCfg),
    Username = get_value(username, ServerCfg),
    Password = get_value(password, ServerCfg),
    Keepalive = get_value(keepalive, ServerCfg),

    RemoteCfg = application:get_env(?APP, remote, []),
    Topics = get_value(topics, RemoteCfg),
    SubQos = get_value(subqos, RemoteCfg),

    LandingMaxQos = application:get_env(?APP, landing_maxqos),
    LandingTopic = application:get_env(?APP, landing_topic),

    %% Preper to connect the remote server
    self() ! connect,

    {ok, #state{host = Host, port = Port, username = Username, password = Password,
                keepalive = Keepalive, topics = Topics, subqos = SubQos,
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
    case OldClient of 
        undefined -> ignore;
        OldClient -> emqttc:disconnect(OldClient)
    end, 
    %% FIXME: connected failed???? {error, Reason}
   {ok, C} = emqttc:start_link([{host, Host},
                                {port, Port},
                                {client_id, ?CLIENT_ID},
                                {username, Username},
                                {password, Password},
                                {keepalive, Keepalive},
                                {logger, {console, info}}]),
    {noreply, State#state{mqttc = C}};

%% Receive Messages
handle_info({publish, Topic, Payload}, #state{landing_maxqos=MaxQos, landing_topic=LTopic} = State) ->
    %% TODO: ?LOG()
    io:format("Message from ~s: ~p~n", [Topic, Payload]),

    %% TODO: Forward the recevied message
    %% FIXME: Qos is not forwarding from remote
    From = <<"emqx_bridge_mqtt">>,
    Msg = emqx_message:make(From, MaxQos, LTopic, Payload),
    emqx:publish(Msg),

    {noreply, State};

%% Client connected
handle_info({mqttc, C, connected}, State = #state{mqttc = C, topics = Topics, subqos = SubQos}) ->
    io:format("Client ~p is connected~n", [C]),
    
    %% Subscribe topics, when the mqttc connected to remote server
    lists:foreach(fun(T) ->
                    emqttc:subscribe(C, T, SubQos)
                  end, Topics),

    {noreply, State};

%% Client disconnected
handle_info({mqttc, C,  disconnected}, State = #state{mqttc = C}) ->
    io:format("Client ~p is disconnected~n", [C]),

    %% TODO: is need to send reconnect????
    self() ! connect,

    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%--------------------------------------------------------------------

%% @doc This module implements EMQX Bridge transport layer on top of MQTT protocol

-module(emqx_bridge_mqtt).

-behaviour(emqx_bridge_connect).

%% behaviour callbacks
-export([ start/1
        , send/2
        , stop/1
        ]).

%% optional behaviour callbacks
-export([ ensure_subscribed/3
        , ensure_unsubscribed/2
        ]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(ACK_REF(ClientPid, PktId), {ClientPid, PktId}).

%% Messages towards ack collector process
-define(REF_IDS(Ref, Ids), {Ref, Ids}).

%%--------------------------------------------------------------------
%% emqx_bridge_connect callbacks
%%--------------------------------------------------------------------

start(Config = #{address := Address}) ->
    Parent = self(),
    Mountpoint = maps:get(receive_mountpoint, Config, undefined),
    Handlers = make_hdlr(Parent, Mountpoint),
    {Host, Port} = case string:tokens(Address, ":") of
                       [H] -> {H, 1883};
                       [H, P] -> {H, list_to_integer(P)}
                   end,
    ClientConfig = Config#{msg_handler => Handlers,
                           host => Host,
                           port => Port,
                           force_ping => true
                          },
    case emqtt:start_link(replvar(ClientConfig)) of
        {ok, Pid} ->
            case emqtt:connect(Pid) of
                {ok, _} ->
                    try
                        subscribe_remote_topics(Pid, maps:get(subscriptions, Config, [])),
                        {ok, #{client_pid => Pid}}
                    catch
                        throw : Reason ->
                            ok = stop(#{client_pid => Pid}),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    ok = stop(#{client_pid => Pid}),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(#{client_pid := Pid}) ->
    safe_stop(Pid, fun() -> emqtt:stop(Pid) end, 1000),
    ok.

ensure_subscribed(#{client_pid := Pid}, Topic, QoS) when is_pid(Pid) ->
    case emqtt:subscribe(Pid, Topic, QoS) of
        {ok, _, _} -> ok;
        Error -> Error
    end;
ensure_subscribed(_Conn, _Topic, _QoS) ->
    %% return ok for now, next re-connect should should call start with new topic added to config
    ok.

ensure_unsubscribed(#{client_pid := Pid}, Topic) when is_pid(Pid) ->
    case emqtt:unsubscribe(Pid, Topic) of
        {ok, _, _} -> ok;
        Error -> Error
    end;
ensure_unsubscribed(_, _) ->
    %% return ok for now, next re-connect should should call start with this topic deleted from config
    ok.

safe_stop(Pid, StopF, Timeout) ->
    MRef = monitor(process, Pid),
    unlink(Pid),
    try
        StopF()
    catch
        _ : _ ->
            ok
    end,
    receive
        {'DOWN', MRef, _, _, _} ->
            ok
    after
        Timeout ->
            exit(Pid, kill)
    end.

send(Conn, Msgs) ->
    send(Conn, Msgs, undefined).
send(_Conn, [], PktId) ->
    {ok, PktId};
send(#{client_pid := ClientPid} = Conn, [Msg | Rest], _PktId) ->
    case emqtt:publish(ClientPid, Msg) of
        ok ->
            Ref = make_ref(),
            self() ! {batch_ack, Ref},
            send(Conn, Rest, Ref);
        {ok, PktId} ->
            send(Conn, Rest, PktId);
        {error, Reason} ->
            %% NOTE: There is no partial sucess of a batch and recover from the middle
            %% only to retry all messages in one batch
            {error, Reason}
    end.


handle_puback(Parent, #{packet_id := PktId, reason_code := RC})
  when RC =:= ?RC_SUCCESS;
       RC =:= ?RC_NO_MATCHING_SUBSCRIBERS ->
    Parent ! {batch_ack, PktId}, ok;
handle_puback(_Parent, #{packet_id := PktId, reason_code := RC}) ->
    ?LOG(warning, "Publish ~p to remote node falied, reason_code: ~p", [PktId, RC]).

handle_publish(Msg, Mountpoint) ->
    emqx_broker:publish(emqx_bridge_msg:to_broker_msg(Msg, Mountpoint)).

handle_disconnected(Parent, Reason) ->
    Parent ! {disconnected, self(), Reason}.

make_hdlr(Parent, Mountpoint) ->
    #{puback => fun(Ack) -> handle_puback(Parent, Ack) end,
      publish => fun(Msg) -> handle_publish(Msg, Mountpoint) end,
      disconnected => fun(Reason) -> handle_disconnected(Parent, Reason) end
     }.

subscribe_remote_topics(ClientPid, Subscriptions) ->
    lists:foreach(fun({Topic, Qos}) ->
                          case emqtt:subscribe(ClientPid, Topic, Qos) of
                              {ok, _, _} -> ok;
                              Error -> throw(Error)
                          end
                  end, Subscriptions).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

replvar(Options) ->
    replvar([clientid], Options).

replvar([], Options) ->
    Options;
replvar([Key|More], Options) ->
    case maps:get(Key, Options, undefined) of
        undefined ->
            replvar(More, Options);
        Val ->
            replvar(More, maps:put(Key, feedvar(Key, Val, Options), Options))
    end.

%% ${node} => node()
feedvar(clientid, ClientId, _) ->
    iolist_to_binary(re:replace(ClientId, "\\${node}", atom_to_list(node())));
feedvar(_, Val, _) ->
    Val.

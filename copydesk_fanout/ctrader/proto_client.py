"""
Shared cTrader Open API connection for this process.

The Open API Python package (ctrader_open_api) is built on Twisted, which is a
single-reactor-per-process framework - there can only be one `reactor.run()` in
this process, and it must run in its own thread since everything else here
(MT5 agents, FastAPI, Supabase calls) is synchronous/threaded, not Twisted.

App-level auth (ProtoOAApplicationAuthReq, using the shared kk5 client_id/secret)
happens exactly once here, on connect. Every CTraderMasterAgent then does its own
account-level auth (ProtoOAAccountAuthReq) on TOP of this one shared TCP connection
- the docs confirm multiple accounts can be authorized on a single app-authed
connection, so there's no need for one socket per master account.

Callers on other threads (agent .start()/.stop(), request/response calls) must
never touch `client`/Twisted objects directly - Twisted objects are not
thread-safe. Use `send_and_wait()` below, which marshals the call onto the
reactor thread via `reactor.callFromThread` and bridges the result back with a
plain `queue.Queue`.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
from typing import Callable

from ctrader_open_api import Client, EndPoints, Protobuf, TcpProtocol
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq,
    ProtoOAErrorRes,
    ProtoOAExecutionEvent,
)
from twisted.internet import reactor

logger = logging.getLogger("ctrader.proto_client")

_DEFAULT_RESPONSE_TIMEOUT_SECONDS = 15.0
_APP_AUTH_TIMEOUT_SECONDS = 20.0


class CTraderConnectionError(Exception):
    """Raised for any failure establishing or using the shared connection."""


def raise_if_error(response, context: str) -> None:
    """
    send_and_wait() decodes whatever the API sends back, success or failure -
    it does NOT raise just because the response is a ProtoOAErrorRes (e.g.
    "account not authorized on this connection", sent after a reconnect that
    hasn't been followed by a fresh ProtoOAAccountAuthReq yet). Every caller
    that reads fields off a specific expected response type (.trader, .deal,
    .symbol, .position, ...) MUST call this first, or an error response causes
    a confusing AttributeError instead of a clear, actionable message.
    """
    if isinstance(response, ProtoOAErrorRes):
        raise CTraderConnectionError(
            f"{context}: cTrader Open API returned an error response "
            f"(errorCode={response.errorCode!r}, description={response.description!r})"
        )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise CTraderConnectionError(
            f"Missing required environment variable {name}. Set CTRADER_CLIENT_ID / "
            f"CTRADER_CLIENT_SECRET from the Open API application (e.g. 'kk5') and "
            f"CTRADER_ENVIRONMENT ('demo' or 'live')."
        )
    return value


class _SharedConnection:
    """
    Process-wide singleton. Do not instantiate directly - use get_connection().
    """

    def __init__(self) -> None:
        self._client: Client | None = None
        self._reactor_thread: threading.Thread | None = None
        self._app_authed = threading.Event()
        self._app_auth_error: str | None = None
        self._lock = threading.Lock()
        # ctidTraderAccountId (int) -> callback(message) for ProtoOAExecutionEvent
        # and anything else that carries that account id and needs live dispatch.
        self._execution_subscribers: dict[int, Callable] = {}
        # Fired after every successful app-level auth EXCEPT the very first one,
        # i.e. specifically on reconnect. App-level auth (ProtoOAApplicationAuthReq)
        # only covers this one shared TCP connection - it says nothing about any
        # individual trading account. Each previously-authed CTraderMasterAgent
        # must redo its own ProtoOAAccountAuthReq after a reconnect, or every
        # subsequent request for that account (ProtoOATraderReq, ProtoOADealListReq,
        # ProtoOASymbolsListReq, ...) comes back as a ProtoOAErrorRes forever.
        self._reconnect_subscribers: list[Callable[[], None]] = []
        self._has_authed_once = False
        # setMessageReceivedCallback (below, in ensure_started) fires _on_message
        # ON THE REACTOR THREAD. Execution-event subscribers (CTraderMasterAgent
        # ._on_message -> _translate_event -> ...) routinely need to make a
        # blocking round-trip back into the API - e.g. symbol_map.SymbolCache
        # lazily resolving a symbolId it hasn't seen yet via send_and_wait(). But
        # send_and_wait() schedules its request with reactor.callFromThread() and
        # then blocks the CALLING thread waiting for the reply; if the calling
        # thread IS the reactor thread, that scheduled call can never run (the
        # reactor is busy blocking on this very frame) and every such call
        # deadlocks until it times out. So execution events are handed off to
        # this dedicated worker thread - never invoked inline from _on_message -
        # and processed in arrival order via a plain queue.
        self._dispatch_queue: queue.Queue = queue.Queue()
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, name="ctrader-dispatch", daemon=True,
        )
        self._dispatch_thread.start()

    # -- lifecycle -------------------------------------------------------- #

    def ensure_started(self) -> None:
        with self._lock:
            if self._client is not None:
                return

            client_id = _require_env("CTRADER_CLIENT_ID")
            client_secret = _require_env("CTRADER_CLIENT_SECRET")
            environment = os.environ.get("CTRADER_ENVIRONMENT", "demo").lower()
            host = (
                EndPoints.PROTOBUF_LIVE_HOST if environment == "live" else EndPoints.PROTOBUF_DEMO_HOST
            )

            self._client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)
            self._client.setConnectedCallback(lambda c: self._on_connected(c, client_id, client_secret))
            self._client.setDisconnectedCallback(self._on_disconnected)
            self._client.setMessageReceivedCallback(self._on_message)

            self._reactor_thread = threading.Thread(
                target=self._run_reactor, name="ctrader-reactor", daemon=True,
            )
            self._reactor_thread.start()

        if not self._app_authed.wait(timeout=_APP_AUTH_TIMEOUT_SECONDS):
            raise CTraderConnectionError(
                "Timed out waiting for cTrader Open API app-level authentication "
                f"(host reachability or bad CTRADER_CLIENT_ID/SECRET are the usual causes)."
            )
        if self._app_auth_error:
            raise CTraderConnectionError(f"App-level authentication failed: {self._app_auth_error}")

    def _run_reactor(self) -> None:
        assert self._client is not None
        self._client.startService()
        # installSignalHandlers=False: this is a background thread, not the main
        # thread, and this process already has its own signal handling (uvicorn/
        # the MT5 agent loop) that must stay in charge of SIGINT/SIGTERM.
        reactor.run(installSignalHandlers=False)

    def _on_connected(self, client: Client, client_id: str, client_secret: str) -> None:
        logger.info("Connected to cTrader Open API host - sending application auth")
        deferred = client.send(
            ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret)
        )
        deferred.addCallbacks(self._on_app_auth_success, self._on_app_auth_failure)

    def _on_app_auth_success(self, _response) -> None:
        logger.info("cTrader Open API application authenticated")
        self._app_auth_error = None
        is_reconnect = self._has_authed_once
        self._has_authed_once = True
        self._app_authed.set()
        if is_reconnect:
            # Notify off the reactor thread: subscribers (CTraderMasterAgent's
            # re-auth handler) call send_and_wait(), which would deadlock the
            # reactor if run inline here.
            threading.Thread(
                target=self._notify_reconnect_subscribers, name="ctrader-reconnect-notify", daemon=True,
            ).start()

    def _notify_reconnect_subscribers(self) -> None:
        for callback in list(self._reconnect_subscribers):
            try:
                callback()
            except Exception:
                logger.exception("Reconnect subscriber raised")

    def _on_app_auth_failure(self, failure) -> None:
        logger.error("cTrader Open API application auth failed: %s", failure)
        self._app_auth_error = str(failure)
        self._app_authed.set()

    def _on_disconnected(self, client: Client, reason) -> None:
        logger.warning("cTrader Open API connection lost: %s - will auto-retry (ClientService)", reason)
        # ClientService (the Client base class) retries the underlying connection
        # on its own; ProtoOAApplicationAuthReq must be re-sent on the next
        # _on_connected, and each CTraderMasterAgent is responsible for noticing
        # its own is_connected go false and re-sending ProtoOAAccountAuthReq plus
        # reconcile() once the app is back up - see master_agent.py.
        self._app_authed.clear()

    def _on_message(self, client: Client, message) -> None:
        try:
            payload = Protobuf.extract(message)
        except Exception:
            logger.exception("Failed to decode incoming cTrader message")
            return

        account_id = getattr(payload, "ctidTraderAccountId", None)
        if account_id is not None and account_id in self._execution_subscribers:
            # Hand off to _dispatch_loop rather than calling the subscriber
            # inline - see the comment on self._dispatch_queue in __init__ for
            # why calling it directly here (on the reactor thread) deadlocks
            # any subscriber code path that calls send_and_wait().
            self._dispatch_queue.put((account_id, payload))

    def _dispatch_loop(self) -> None:
        while True:
            account_id, payload = self._dispatch_queue.get()
            callback = self._execution_subscribers.get(account_id)
            if callback is None:
                # Unsubscribed between enqueue and dispatch (e.g. agent.stop()) -
                # nothing to deliver to.
                continue
            try:
                callback(payload)
            except Exception:
                logger.exception("Execution subscriber for ctid %s raised", account_id)

    # -- subscriber registration ------------------------------------------ #

    def subscribe(self, ctid_trader_account_id: int, callback: Callable) -> None:
        self._execution_subscribers[ctid_trader_account_id] = callback

    def unsubscribe(self, ctid_trader_account_id: int) -> None:
        self._execution_subscribers.pop(ctid_trader_account_id, None)

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """
        Register `callback` to be invoked (on a background thread, never the
        reactor thread) after every reconnect - i.e. every successful
        ProtoOAApplicationAuthReq after the first. Used by CTraderMasterAgent
        to redo its ProtoOAAccountAuthReq once the shared connection comes
        back up. Not called for the initial connection - callers that need
        the initial auth do it themselves in start().
        """
        self._reconnect_subscribers.append(callback)

    # -- request/response bridge ------------------------------------------ #

    def send_and_wait(self, message, timeout: float = _DEFAULT_RESPONSE_TIMEOUT_SECONDS):
        """
        Send a protobuf request and block the CALLING (non-reactor) thread until
        a response arrives or `timeout` elapses. Safe to call from any thread.

        client.send()'s deferred resolves with the raw, undecoded ProtoMessage
        wrapper (confirmed by reading ctrader_open_api.client.Client._received(),
        which does `responseDeferred.callback(message)` with no decoding step) -
        the same as what arrives in _on_message above. Protobuf.extract() is
        applied here, once, so every caller gets the properly typed response
        (e.g. ProtoOAGetAccountListByAccessTokenRes) instead of a bare
        ProtoMessage with none of the expected fields.
        """
        self.ensure_started()
        assert self._client is not None
        result_queue: queue.Queue = queue.Queue(maxsize=1)

        def _do_send():
            deferred = self._client.send(message, responseTimeoutInSeconds=timeout)
            deferred.addCallbacks(
                lambda res: result_queue.put(("ok", res)),
                lambda err: result_queue.put(("err", err)),
            )

        reactor.callFromThread(_do_send)

        try:
            status, value = result_queue.get(timeout=timeout + 2.0)
        except queue.Empty:
            raise CTraderConnectionError(
                f"No response from cTrader Open API within {timeout}s for {type(message).__name__}"
            )
        if status == "err":
            raise CTraderConnectionError(f"cTrader Open API request failed: {value}")
        try:
            return Protobuf.extract(value)
        except Exception as exc:
            raise CTraderConnectionError(
                f"Failed to decode cTrader Open API response to {type(message).__name__}: {exc}"
            ) from exc


_connection = _SharedConnection()


def get_connection() -> _SharedConnection:
    _connection.ensure_started()
    return _connection
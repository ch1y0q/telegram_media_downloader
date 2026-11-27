"""Exact-message forwarding API; no command parsing, runtime config or startup I/O."""

import asyncio
import math
import os
import re
import shutil
import tempfile
import time
import traceback
import uuid
from collections import defaultdict
from contextlib import ExitStack
from copy import copy
from functools import partial
from typing import Any, Iterable, Optional

MAX_MESSAGES = 10000
MAX_MESSAGE_ID = 2**31 - 1
GROUP_LIMIT = 10
MAX_FLOOD_WAIT_SECONDS = 60
MEDIA_KINDS = (
    "photo",
    "video",
    "animation",
    "audio",
    "voice",
    "video_note",
    "sticker",
    "document",
)
FAMILIES = {
    "photo": "visual",
    "video": "visual",
    "document": "document",
    "audio": "audio",
}


def failure_detail(exc, stage):
    """Diagnostics deliberately omit exception text, source lines and locals."""
    return {"error": type(exc).__name__, "stage": stage, "retryable": stage == "read",
            "diagnostic": uuid.uuid4().hex[:12],
            "frames": [f"{os.path.basename(frame.filename)}:{frame.lineno}:{frame.name}"
                       for frame in traceback.extract_tb(exc.__traceback__)[-8:]]}


def media_kind(message) -> Optional[str]:
    """Return the selected message's file-media kind, or None for other content."""
    return next(
        (kind for kind in MEDIA_KINDS if getattr(message, kind, None) is not None), None
    )


def _available(message):
    return (
        message is not None
        and not getattr(message, "empty", False)
        and not getattr(message, "service", None)
    )


def _chat_id(message):
    return getattr(getattr(message, "chat", None), "id", None)


class _Planner:
    """Incremental so exact-ID fetch boundaries never become album boundaries."""

    def __init__(self, group_mode):
        if group_mode not in ("keep", "split", "force"):
            raise ValueError("invalid group mode")
        self.mode = group_mode
        self.pending: list[Any] = []
        self.key = None

    def feed(self, message) -> list[list[Any]]:
        key = None
        if _available(message) and self.mode != "split":
            chat = _chat_id(message)
            album = (
                getattr(message, "media_group_id", None)
                if self.mode == "keep"
                else True
            )
            family = FAMILIES.get(media_kind(message) or "")
            if chat and album and family:
                key = chat, album, family
        ready = []
        if self.pending and key != self.key:
            ready.append(self.pending)
            self.pending = []
        if _available(message):
            self.pending.append(message)
            if key is None or len(self.pending) == GROUP_LIMIT:
                ready.append(self.pending)
                self.pending = []
        self.key = key
        return ready


def plan_units(messages: Iterable[Any], group_mode: str = "keep") -> list[list[Any]]:
    """Partition only supplied items, in order. Missing/empty/service slots are barriers."""
    planner = _Planner(group_mode)
    units = [unit for message in messages for unit in planner.feed(message)]
    return units + planner.feed(None)


def validate_request(src, dst, list_of_msg_ids, *, whole_dialog=False):
    """Validate the execution contract before any Telegram operation."""
    for index, peer in enumerate((src, dst)):
        if type(peer) is int:
            if not 0 < abs(peer) < 2**63 or (index == 1 and peer > 0):
                raise ValueError(
                    "expected a source peer or negative destination chat ID"
                )
        elif not isinstance(peer, str) or not re.fullmatch(
            r"@[A-Za-z][A-Za-z0-9_]{3,31}", peer
        ):
            raise ValueError("expected a normalized chat ID or username")
    if str(src).lower() == str(dst).lower():
        raise ValueError("source and destination must differ")
    if type(whole_dialog) is not bool:
        raise ValueError("invalid whole-dialog option")
    if (
        not isinstance(list_of_msg_ids, list)
        or not list_of_msg_ids
        or (not whole_dialog and len(list_of_msg_ids) > MAX_MESSAGES)
    ):
        raise ValueError("expected a bounded nonempty message ID list")
    if any(
        type(mid) is not int or not 1 <= mid <= MAX_MESSAGE_ID
        for mid in list_of_msg_ids
    ):
        raise ValueError("invalid message ID")
    if len(set(list_of_msg_ids)) != len(list_of_msg_ids):
        raise ValueError("message IDs must be unique")
    if whole_dialog and (
        type(src) is not int
        or type(dst) is not int
        or list_of_msg_ids != sorted(list_of_msg_ids)
    ):
        raise ValueError("whole-dialog plans require numeric peers and ordered IDs")


class _TransferProgress:
    """Optional telemetry must never turn an accepted send into a failed send."""

    def __init__(self, callback, *, unit_size=0, flood=None):
        self.callback = callback
        self.flood = flood
        self.unit_size = unit_size
        self.file_index = 0
        self.files = {}
        self.state = {}
        self.started = time.monotonic()
        self.last_emit = float("-inf")

    async def stage(self, name, **values):
        self.started = time.monotonic()
        self.state = dict(stage=name, **values)
        await self.emit(force=True)

    async def emit(self, force=False):
        now = time.monotonic()
        if self.callback is None or (not force and now - self.last_emit < 2):
            return
        self.last_emit = now
        try:
            await self.callback(dict(self.state, elapsed=max(0, now - self.started), unit_size=self.unit_size, file_index=self.file_index))
        except Exception:
            pass  # Progress display is advisory; cancellation still propagates.

    async def bytes(self, current, total):
        expected = self.state.get("total") or total or 0
        if current < self.state.get("current", 0):
            self.started = time.monotonic()  # SDK restarted a transfer.
        self.state.update(current=min(current, expected) if expected else current,
                          total=expected)
        await self.emit()


class _FloodDeferred(Exception):
    def __init__(self, retry_at, *, safe_to_resume=False):
        super().__init__('Telegram rejected the request until the cooldown expires')
        self.retry_at = retry_at
        self.safe_to_resume = safe_to_resume


class _FloodControl:
    def __init__(self, budget, should_pause):
        self.remaining = budget
        self.should_pause = should_pause

    async def wait(self, delay, attempt, *, safe_to_resume):
        retry_at = math.ceil(time.time() + delay)
        deferred = _FloodDeferred(retry_at, safe_to_resume=safe_to_resume)
        if attempt == 2 or delay > self.remaining:
            raise deferred
        self.remaining -= delay
        if self.should_pause is None:
            await asyncio.sleep(delay)
            return
        for start in range(0, delay, 2):
            if self.should_pause():
                raise deferred
            await asyncio.sleep(min(2, delay - start))
        if self.should_pause():
            raise deferred


async def _flood_retry(operation, progress=None, *, safe_to_resume=False):
    managed = progress.flood if progress is not None else None
    for attempt in range(3):
        try:
            return await operation()
        except Exception as exc:
            if getattr(exc, "ID", "") not in {
                "FLOOD_WAIT", "FLOOD_WAIT_X", "FLOOD_PREMIUM_WAIT", "FLOOD_PREMIUM_WAIT_X"
            }:
                raise
            delay = max(1, int(getattr(exc, "value", 1)))
            if managed is None and (attempt == 2 or delay > MAX_FLOOD_WAIT_SECONDS):
                raise
            previous = None
            if progress is not None:
                previous = (dict(progress.state), progress.started)
                await progress.stage("flood_wait", seconds=delay)
            if managed is None:
                await asyncio.sleep(delay)
            else:
                await managed.wait(delay, attempt, safe_to_resume=safe_to_resume)
            if previous is not None:
                progress.state, progress.started = previous
                await progress.emit(force=True)
    raise RuntimeError("retry exhausted")


class _AlbumCaption(str):
    """Carry original entities through SDK versions that parse only item.caption."""

    entities: Any


async def _once(operation):
    return await operation()


def _unit_client(client, progress=None):
    """Scope SDK retry/parser changes to this send, never mutate a shared client."""
    from pyrogram import Client, enums, utils

    if not isinstance(client, Client):
        return client
    scoped = copy(client)
    scoped.parser = copy(client.parser)
    scoped.message_cache = defaultdict(lambda: None)

    async def omit_messages(
        chat_id, message_ids=None, reply_to_message_ids=None, **kwargs
    ):
        # Pyrogram can request pinned/replied-to messages even with replies=0.
        ids = message_ids if message_ids is not None else reply_to_message_ids
        return None if isinstance(ids, int) else []

    async def omit_metadata(*args, **kwargs):
        return None

    async def parse(text, mode=None):
        if mode == enums.ParseMode.DISABLED:
            if isinstance(text, _AlbumCaption) and text.entities:
                return await utils.parse_text_entities(
                    scoped, str(text), mode, text.entities
                )
            return {"message": str(text or ""), "entities": None}
        return await client.parser.parse(text, mode)

    async def invoke(query, **kwargs):
        kwargs.update(retries=0, sleep_threshold=0)
        # Only these destination RPCs can prove that the actual send was rejected.
        rejected_send_is_safe = type(query).__name__ in {
            "SendMessage", "SendMedia", "SendMultiMedia", "ForwardMessages"
        }
        return await _flood_retry(partial(client.invoke, query, **kwargs), progress,
                                  safe_to_resume=rejected_send_is_safe)

    if progress is not None:
        original_save = scoped.save_file

        async def save_file(path, *args, **kwargs):
            # Album uploads call save_file directly and expose no progress argument.
            # Ignore optional absent thumbnails, preserving the main-file status.
            if path is None:
                return await original_save(path, *args, **kwargs)
            progress.file_index = progress.files.get(path, progress.file_index)
            await progress.stage("upload", current=0,
                                 total=os.path.getsize(path) if isinstance(path, str) else 0)
            kwargs.update(progress=progress.bytes, progress_args=())
            value = await original_save(path, *args, **kwargs)
            await progress.stage("confirm")
            return value

        scoped.save_file = save_file

    scoped.parser.parse = parse
    scoped.invoke = invoke
    scoped.get_messages = omit_messages
    scoped.get_stories = scoped.get_forum_topics_by_id = omit_metadata
    return scoped


def _caption(message):
    from pyrogram.enums import ParseMode

    entities = getattr(message, "caption_entities", None)
    return dict(
        caption=getattr(message, "caption", None) or "",
        # Pyrogram injects _client into entities while serializing.
        caption_entities=[copy(e) for e in entities] if entities is not None else None,
        parse_mode=ParseMode.DISABLED,
    )


def _media_options(message, kind):
    fields = {
        "video": ("width", "height", "duration", "supports_streaming"),
        "audio": ("duration", "performer", "title"),
        "voice": ("duration",),
        "animation": ("width", "height", "duration"),
    }
    media = getattr(message, kind)
    kwargs = {
        key: getattr(media, key)
        for key in fields.get(kind, ())
        if getattr(media, key, None) is not None
    }
    spoiler = getattr(message, "has_media_spoiler", None)
    if kind in ("photo", "video", "animation") and spoiler is not None:
        kwargs["has_spoiler"] = spoiler
    if kind not in ("video_note", "sticker"):
        kwargs.update(_caption(message))
    return kwargs


async def _download(client, message, kind, folder, progress=None):
    # Separate private folders allow equal basenames without overwriting a member.
    media = getattr(message, kind)
    # Telegram Photo has no file_name; UploadMedia rejects a nameless extension.
    # Photos are downloaded as JPEG by the SDK. Keep document filenames intact.
    fallback = "photo.jpg" if kind == "photo" else "media"
    name = os.path.basename(getattr(media, "file_name", None) or fallback)
    if kind == "photo" or name in ("", ".", ".."):
        name = fallback
    target = os.path.join(folder, name)
    expected = getattr(media, 'file_size', 0) or 0
    if shutil.disk_usage(folder).free < expected + 64 * 1024 * 1024:
        raise OSError('insufficient temporary disk space')
    if progress is not None:
        progress.files[target] = progress.file_index
    # Pass the exact media, not the message: some SDKs prefer its replied-to story.
    options = {}
    if progress is not None:
        await progress.stage("download", current=0, total=getattr(media, "file_size", 0) or 0)
        options["progress"] = progress.bytes
    path = await _flood_retry(partial(client.download_media, media, file_name=target, **options), progress)
    if (
        not isinstance(path, str)
        or os.path.realpath(path) != os.path.realpath(target)
        or os.path.dirname(os.path.realpath(path)) != os.path.realpath(folder)
        or os.path.islink(path)
        or not os.path.isfile(path)
    ):
        raise ValueError("download did not produce the requested file")
    size, expected = os.path.getsize(path), getattr(media, "file_size", None)
    if size <= 0 or (isinstance(expected, int) and expected > 0 and expected != size):
        raise ValueError("download size mismatch")
    return path


def _cached_id(message, kind):
    value = getattr(getattr(message, kind), "file_id", None)
    # send_media_group also accepts URLs and local paths; neither is a cached ID.
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[A-Za-z0-9_-]+", value)
        or os.path.isfile(value)
    ):
        raise ValueError("invalid cached media ID")
    return value


def _input_media(message, kind, value):
    from pyrogram.types import (
        InputMediaAudio,
        InputMediaDocument,
        InputMediaPhoto,
        InputMediaVideo,
    )

    constructors = {
        "photo": InputMediaPhoto,
        "video": InputMediaVideo,
        "document": InputMediaDocument,
        "audio": InputMediaAudio,
    }
    kwargs = _media_options(message, kind)
    caption = _AlbumCaption(kwargs["caption"])
    caption.entities = kwargs["caption_entities"]
    kwargs["caption"] = caption
    return constructors[kind](media=value, **kwargs)


class UnconfirmedSendError(RuntimeError):
    """A send may have succeeded; do not retry or delete the corresponding inputs."""


def _confirmed(response, count, dst) -> list[Any]:
    sent = list(response) if isinstance(response, (list, tuple)) else [response]
    ids = [getattr(item, "id", None) for item in sent]
    if (
        len(ids) != count
        or any(type(mid) is not int or mid <= 0 for mid in ids)
        or len(set(ids)) != count
        or any(getattr(item, "empty", False) for item in sent)
        or (
            type(dst) is int and any(_chat_id(item) not in (None, dst) for item in sent)
        )
    ):
        raise UnconfirmedSendError("send outcome not confirmed")
    return sent


async def send_unit(
    client, messages, dst, *, drop_author=True, protected=False, on_status=None, flood=None
) -> list[Any]:
    """Send an already received/planned unit exactly once except explicit FloodWait."""
    unit = list(messages)
    if type(drop_author) is not bool or type(protected) is not bool:
        raise ValueError("invalid forwarding options")
    if not 1 <= len(unit) <= GROUP_LIMIT or not all(_available(m) for m in unit):
        raise ValueError("invalid send unit")
    ids = [m.id for m in unit]
    if any(type(mid) is not int or mid <= 0 for mid in ids) or len(set(ids)) != len(
        ids
    ):
        raise ValueError("invalid unit IDs")
    if len(plan_units(unit, "force" if drop_author else "keep")) != 1:
        raise ValueError("incompatible send unit")
    protected_media = [
        bool(
            protected
            or getattr(m, "has_protected_content", False)
            or getattr(getattr(m, "chat", None), "has_protected_content", False)
        )
        and bool(media_kind(m) or getattr(m, "media", None))
        for m in unit
    ]
    if any(protected_media) and not drop_author:
        raise ValueError("protected media cannot retain its forwarding header")
    progress = _TransferProgress(on_status, unit_size=len(unit), flood=flood) if on_status is not None or flood else None
    if any(protected_media):
        required = sum(getattr(getattr(m, media_kind(m), None), 'file_size', 0) or 0 for m,needed in zip(unit,protected_media) if needed)
        if shutil.disk_usage(tempfile.gettempdir()).free < required + 64 * 1024 * 1024:
            raise OSError('insufficient temporary disk space')
    scoped = _unit_client(client, progress)
    if progress is not None:
        progress.file_index = 1
        await progress.stage("copy")
    # Real SDK RPCs retry in scoped.invoke. Never replay a high-level send if
    # result parsing encounters a FloodWait *after* Telegram accepted the send.
    send = partial(_flood_retry, progress=progress) if scoped is client else _once
    if len(unit) == 1:
        m = unit[0]
        kind = media_kind(m)
        if protected_media[0]:
            if kind is None:
                raise ValueError("unsupported protected media")
            with tempfile.TemporaryDirectory(prefix="telegram-forward-") as folder:
                path = await _download(scoped, m, kind, folder, progress)
                response = await send(
                    partial(
                        getattr(scoped, "send_" + kind),
                        dst,
                        path,
                        **_media_options(m, kind)
                    )
                )
        else:
            if scoped is not client:
                m = copy(m)
                m._client = scoped
                if getattr(m, "entities", None):
                    m.entities = [copy(e) for e in m.entities]
            options = (
                _caption(m) if kind and kind not in ("sticker", "video_note") else {}
            )
            operation = (
                partial(m.copy, dst, reply_markup=None, **options)
                if drop_author
                else partial(m.forward, dst)
            )
            response = await send(operation)
    elif not drop_author:
        response = await send(
            partial(scoped.forward_messages, dst, _chat_id(unit[0]), ids)
        )
    else:
        with ExitStack() as stack:
            media = []
            for index, (m, download) in enumerate(zip(unit, protected_media), 1):
                if progress is not None:
                    progress.file_index = index
                kind = media_kind(m)
                if download:
                    folder = stack.enter_context(
                        tempfile.TemporaryDirectory(prefix="telegram-forward-")
                    )
                    value = await _download(scoped, m, kind, folder, progress)
                else:
                    value = _cached_id(m, kind)
                media.append(_input_media(m, kind, value))
            response = await send(partial(scoped.send_media_group, dst, media))
    confirmed = _confirmed(response, len(unit), dst)
    if scoped is not client:
        for item in confirmed:
            if getattr(item, "_client", None) is scoped:
                item._client = client
    return confirmed


async def _selected(client, source_id, ids, batch_size, progress=None):
    """Yield one slot per requested ID, including fetch failures and missing slots."""
    for start in range(0, len(ids), batch_size):
        batch = ids[start : start + batch_size]
        try:
            if progress is not None:
                await progress.stage("read")
            scoped = _unit_client(client, progress)
            # Enter get_messages exactly once for this batch; any nested calls made
            # by its result parser hit the instance-level omit_messages instead.
            fetch = (
                client.get_messages
                if scoped is client
                else partial(type(scoped).get_messages, scoped)
            )
            read = partial(_flood_retry, progress=progress) if scoped is client else _once
            fetched = await read(
                partial(fetch, source_id, message_ids=batch, replies=0)
            )
            fetched = fetched if isinstance(fetched, (list, tuple)) else [fetched]
            found = {}
            other_chat = set()
            for m in fetched:
                if m is None or getattr(m, "empty", False):
                    continue
                if type(m.id) is not int or m.id not in batch or m.id in found:
                    raise ValueError("unexpected source message")
                if _chat_id(m) not in (None, source_id):
                    # Non-channel IDs are account-wide. GetMessages may return
                    # another dialog's item inside an explicitly selected range.
                    # Keep its slot as a barrier, without rejecting valid peers.
                    other_chat.add(m.id)
                    found[m.id] = None
                else:
                    found[m.id] = m
        except _FloodDeferred:
            raise
        except Exception as exc:
            detail = failure_detail(exc, "read")
            for mid in batch:
                yield mid, None, detail
            continue
        for mid in batch:
            m = found.get(mid)
            yield mid, m if _available(m) else None, (
                "SourceChatMismatch" if mid in other_chat else None
            )


async def forward_messages(
    src,
    dst,
    list_of_msg_ids,
    *,
    client,
    sequential=False,
    drop_author=True,
    group_mode="keep",
    on_progress=None,
    whole_dialog=False,
    on_status=None,
    should_pause=None,
    flood_wait_budget=None,
):
    """Forward exact IDs with an already connected client.

    The caller owns authorization, session exclusivity and cancellation. No history,
    dialog listing, reply expansion, album expansion, source deletion or config writes.
    """
    validate_request(src, dst, list_of_msg_ids, whole_dialog=whole_dialog)
    if type(sequential) is not bool or type(drop_author) is not bool:
        raise ValueError("invalid forwarding options")
    planner = _Planner(group_mode)
    if group_mode == "force" and not drop_author:
        raise ValueError("force grouping requires drop_author=True")
    list_of_msg_ids = list(list_of_msg_ids)
    flood = _FloodControl(flood_wait_budget, should_pause) if flood_wait_budget is not None else None
    transfer = _TransferProgress(on_status, flood=flood) if on_status is not None or flood else None
    result = {"requested": len(list_of_msg_ids), "sent": 0, "missing": [], "failed": [], "receipts": []}
    batch_size = 1 if sequential and group_mode == "split" else 100

    async def progress():
        if on_progress is not None:
            snapshot = dict(
                result, failed=list(result["failed"]), missing=list(result["missing"]), receipts=list(result["receipts"])
            )
            if "skipped_other_chat" in result:
                snapshot["skipped_other_chat"] = list(result["skipped_other_chat"])
            await on_progress(snapshot)

    def pause_result(flood_error=None):
        done = result['sent'] + len(result['missing']) + len(result['failed'])
        pending = list_of_msg_ids[done:]
        result['missing'].extend(pending)
        result.update(paused=bool(pending), pending=pending)
        if flood_error is not None:
            result['flood_retry_at'] = flood_error.retry_at
        return result

    async def deliver(unit):
        try:
            sent = await send_unit(
                client,
                unit,
                destination.id,
                drop_author=drop_author,
                protected=protected,
                **({"on_status": on_status} if on_status is not None else {}),
                **({"flood": flood} if flood is not None else {}),
            )
            result["sent"] += len(sent)
            result["receipts"].extend({"source": original.id, "chat": destination.id, "id": target.id}
                                      for original, target in zip(unit, sent))
        except _FloodDeferred as exc:
            if not exc.safe_to_resume:
                detail = failure_detail(exc, "send")
                result["failed"].extend(dict(detail, id=m.id) for m in unit)
            raise
        except Exception as exc:
            detail = failure_detail(exc, "send")
            result["failed"].extend(dict(detail, id=m.id) for m in unit)
        await progress()

    try:
        if transfer is not None:
            await transfer.stage("read")
        resolve = partial(_flood_retry, progress=transfer) if flood else _once
        source = await resolve(partial(client.get_chat, src, force_full=False))
        destination = await resolve(partial(client.get_chat, dst, force_full=False))
        if source.id == destination.id:
            raise ValueError("source and destination must differ")
        protected = bool(getattr(source, "has_protected_content", False))
        async for mid, message, error in _selected(
            client, source.id, list_of_msg_ids, batch_size, transfer
        ):
            if should_pause is not None and should_pause():
                return pause_result()
            for unit in planner.feed(message):
                await deliver(unit)
                if should_pause is not None and should_pause():
                    return pause_result()
            if message is None:
                if error == "SourceChatMismatch":
                    result["missing"].append(mid)
                    result.setdefault("skipped_other_chat", []).append(mid)
                elif error:
                    result["failed"].append(dict(error, id=mid))
                else:
                    result["missing"].append(mid)
                await progress()
        for unit in planner.feed(None):
            if should_pause is not None and should_pause():
                return pause_result()
            await deliver(unit)
        return result
    except _FloodDeferred as exc:
        return pause_result(exc)

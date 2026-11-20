"""Exact-message forwarding API; no command parsing, runtime config or startup I/O."""

import asyncio
import os
import re
import tempfile

MAX_MESSAGES = 10000
MAX_MESSAGE_ID = 2**31 - 1


def validate_request(src, dst, list_of_msg_ids):
    """Validate the execution contract before any Telegram operation."""
    for peer in (src, dst):
        if type(peer) is int:
            if not -(2**63) < peer < 0:
                raise ValueError("expected a negative chat ID")
        elif not isinstance(peer, str) or not re.fullmatch(
            r"@[A-Za-z][A-Za-z0-9_]{3,31}", peer
        ):
            raise ValueError("expected a normalized chat ID or username")
    if str(src).lower() == str(dst).lower():
        raise ValueError("source and destination must differ")
    if (
        not isinstance(list_of_msg_ids, list)
        or not 1 <= len(list_of_msg_ids) <= MAX_MESSAGES
    ):
        raise ValueError("expected a bounded nonempty message ID list")
    if any(
        type(mid) is not int or not 1 <= mid <= MAX_MESSAGE_ID
        for mid in list_of_msg_ids
    ):
        raise ValueError("invalid message ID")
    if len(set(list_of_msg_ids)) != len(list_of_msg_ids):
        raise ValueError("message IDs must be unique")


async def _flood_retry(operation):
    for attempt in range(3):
        try:
            return await operation()
        except Exception as exc:
            if (
                getattr(exc, "ID", "") not in {"FLOOD_WAIT", "FLOOD_PREMIUM_WAIT"}
                or attempt == 2
            ):
                raise
            await asyncio.sleep(max(1, int(getattr(exc, "value", 1))))
    raise RuntimeError("retry exhausted")


async def _send_one(client, message, dst, drop_author, protected):
    if not protected or not getattr(message, "media", None):
        if drop_author:
            return await message.copy(dst, reply_markup=None)
        return await message.forward(dst)
    if not drop_author:
        raise ValueError("protected media cannot retain its forwarding header")
    # Never request an album or download any message besides this selected item.
    kind = next(
        (
            name
            for name in (
                "photo",
                "video",
                "document",
                "audio",
                "animation",
                "voice",
                "video_note",
                "sticker",
            )
            if getattr(message, name, None) is not None
        ),
        None,
    )
    if kind is None:
        raise ValueError("unsupported media")
    with tempfile.TemporaryDirectory(prefix="telegram-forward-") as folder:
        path = await client.download_media(
            message, file_name=os.path.join(folder, "media")
        )
        if (
            not isinstance(path, str)
            or os.path.dirname(os.path.realpath(path)) != os.path.realpath(folder)
            or not os.path.isfile(path)
            or os.path.getsize(path) <= 0
        ):
            raise ValueError("download did not produce a valid file")
        expected = getattr(getattr(message, kind), "file_size", None)
        if (
            isinstance(expected, int)
            and expected > 0
            and os.path.getsize(path) != expected
        ):
            raise ValueError("download size mismatch")
        kwargs = {}
        if kind not in {"video_note", "sticker"}:
            kwargs.update(
                caption=message.caption or "",
                caption_entities=message.caption_entities,
                parse_mode="disabled",
            )
        return await getattr(client, "send_" + kind)(dst, path, **kwargs)


async def forward_messages(
    src,
    dst,
    list_of_msg_ids,
    *,
    client,
    sequential=False,
    drop_author=True,
    on_progress=None
):
    """Forward exact IDs with an already connected client.

    The caller owns authorization, session exclusivity and cancellation. No history,
    dialog listing, reply expansion, album expansion, source deletion or config writes.
    """
    validate_request(src, dst, list_of_msg_ids)
    if type(sequential) is not bool or type(drop_author) is not bool:
        raise ValueError("invalid forwarding options")
    list_of_msg_ids = list(list_of_msg_ids)
    source = await client.get_chat(src, force_full=False)
    destination = await client.get_chat(dst, force_full=False)
    if source.id == destination.id:
        raise ValueError("source and destination must differ")
    protected = bool(getattr(source, "has_protected_content", False))
    result = {"requested": len(list_of_msg_ids), "sent": 0, "missing": [], "failed": []}
    batch_size = 1 if sequential else 100
    for start in range(0, len(list_of_msg_ids), batch_size):
        batch = list_of_msg_ids[start : start + batch_size]
        try:
            messages = await _flood_retry(
                lambda: client.get_messages(source.id, message_ids=batch, replies=0)
            )
            messages = messages if isinstance(messages, (list, tuple)) else [messages]
            found = {
                m.id: m
                for m in messages
                if m is not None and not getattr(m, "empty", False)
            }
            if any(mid not in batch for mid in found):
                raise ValueError("response contains unrequested messages")
        except Exception as exc:
            result["failed"].extend(
                {"id": mid, "error": type(exc).__name__} for mid in batch
            )
            found = None
        for mid in batch:
            if found is not None:
                message = found.get(mid)
                if message is None or getattr(message, "service", None):
                    result["missing"].append(mid)
                else:
                    try:
                        sent = await _flood_retry(
                            lambda: _send_one(
                                client,
                                message,
                                destination.id,
                                drop_author,
                                protected
                                or bool(
                                    getattr(message, "has_protected_content", False)
                                ),
                            )
                        )
                        if not sent:
                            raise RuntimeError("send outcome not confirmed")
                        result["sent"] += 1
                    except Exception as exc:
                        # Only explicit FloodWait is retryable. A transport failure
                        # may have delivered already, so never retry it automatically.
                        result["failed"].append(
                            {"id": mid, "error": type(exc).__name__}
                        )
            if on_progress is not None:
                await on_progress(dict(result))
    return result

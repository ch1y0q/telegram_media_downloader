"""Offline exact-ID API tests: deliberately no application or Telegram startup."""

import asyncio
import importlib.util
import socket
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

SOURCE = Path(__file__).resolve().parents[1] / "module/forwarding.py"


def load_executor():
    spec = importlib.util.spec_from_file_location("forwarding_offline", SOURCE)
    module = importlib.util.module_from_spec(spec)
    with patch(
        "builtins.open", side_effect=AssertionError("runtime file read")
    ), patch.object(socket, "socket", side_effect=AssertionError("network")):
        spec.loader.exec_module(module)
    return module


def message(mid, **kwargs):
    values = dict(
        id=mid,
        empty=False,
        service=None,
        media=None,
        copy=AsyncMock(return_value=object()),
        forward=AsyncMock(return_value=object()),
        caption="fixture",
        caption_entities=[],
    )
    values.update(kwargs)
    return types.SimpleNamespace(**values)


class ForwardingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.api = load_executor()
        self.messages = {}
        self.client = types.SimpleNamespace(
            get_chat=AsyncMock(
                side_effect=lambda peer, **kw: types.SimpleNamespace(
                    id=-1001 if peer == "@source" else -1002,
                    has_protected_content=False,
                )
            ),
            get_messages=AsyncMock(
                side_effect=lambda peer, message_ids, replies: [
                    self.messages.setdefault(mid, message(mid)) for mid in message_ids
                ]
            ),
            download_media=AsyncMock(),
            send_video=AsyncMock(return_value=object()),
            send_document=AsyncMock(return_value=object()),
        )

    async def run_request(self, ids=None, **kwargs):
        return await self.api.forward_messages(
            "@source",
            "@target",
            [123, 124, 126, 127] if ids is None else ids,
            client=self.client,
            **kwargs
        )

    async def test_exact_selection_and_metadata_only(self):
        result = await self.run_request()
        self.assertEqual(result, dict(requested=4, sent=4, missing=[], failed=[]))
        self.client.get_messages.assert_awaited_once_with(
            -1001, message_ids=[123, 124, 126, 127], replies=0
        )
        self.assertEqual(
            self.client.get_chat.call_args_list[0].kwargs, {"force_full": False}
        )
        self.assertEqual(
            self.client.get_chat.call_args_list[1].kwargs, {"force_full": False}
        )
        for msg in self.messages.values():
            msg.copy.assert_awaited_once_with(-1002, reply_markup=None)
            msg.forward.assert_not_awaited()
        self.client.download_media.assert_not_awaited()

    async def test_batches_never_expand_holes(self):
        ids = list(range(1, 403, 2))
        result = await self.run_request(ids)
        calls = self.client.get_messages.call_args_list
        self.assertEqual(
            [len(call.kwargs["message_ids"]) for call in calls], [100, 100, 1]
        )
        self.assertEqual(
            [mid for call in calls for mid in call.kwargs["message_ids"]], ids
        )
        self.assertEqual(result["sent"], len(ids))

    async def test_sequential_waits_for_send_before_next_read(self):
        events, entered, release = [], asyncio.Event(), asyncio.Event()

        async def copy(dst, **kwargs):
            events.append("send-start")
            entered.set()
            await release.wait()
            events.append("send-done")
            return object()

        self.messages[7] = message(7, copy=copy)
        task = asyncio.create_task(self.run_request([7, 3], sequential=True))
        await entered.wait()
        self.assertEqual(self.client.get_messages.await_count, 1)
        release.set()
        result = await task
        self.assertEqual(events, ["send-start", "send-done"])
        self.assertEqual(
            [
                call.kwargs["message_ids"]
                for call in self.client.get_messages.call_args_list
            ],
            [[7], [3]],
        )
        self.assertEqual(result["sent"], 2)

    async def test_forward_header_option(self):
        await self.run_request([4], drop_author=False)
        self.messages[4].forward.assert_awaited_once_with(-1002)
        self.messages[4].copy.assert_not_awaited()

    async def test_validation_before_any_client_access(self):
        cases = [[], [0], [-1], [True], ["1"], [1, 1], [2**31], list(range(1, 10002))]
        for ids in cases:
            with self.subTest(ids=str(ids)[:40]), self.assertRaises(ValueError):
                await self.run_request(ids)
        for src, dst in [
            (True, "@target"),
            (1, "@target"),
            ("me", "@target"),
            ("@bad/user", "@target"),
            ("@source", "@SOURCE"),
            (-1, -1),
        ]:
            with self.assertRaises(ValueError):
                await self.api.forward_messages(src, dst, [1], client=self.client)
        with self.assertRaises(ValueError):
            await self.run_request([1], sequential=1)
        self.client.get_chat.assert_not_awaited()

    async def test_same_resolved_chat_fails_before_messages(self):
        self.client.get_chat.side_effect = None
        self.client.get_chat.return_value = types.SimpleNamespace(id=-1)
        with self.assertRaises(ValueError):
            await self.run_request()
        self.client.get_messages.assert_not_awaited()

    async def test_missing_and_service_messages_reported_without_expansion(self):
        self.client.get_messages.side_effect = None
        self.client.get_messages.return_value = [
            message(1),
            message(2, empty=True),
            message(3, service=True),
        ]
        result = await self.run_request([1, 2, 3, 9])
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["missing"], [2, 3, 9])
        self.client.get_messages.assert_awaited_once()

    async def test_unrequested_response_never_sent(self):
        extra = message(99)
        self.client.get_messages.side_effect = None
        self.client.get_messages.return_value = [message(1), extra]
        result = await self.run_request([1])
        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["failed"], [{"id": 1, "error": "ValueError"}])
        extra.copy.assert_not_awaited()

    async def test_fetch_failure_has_no_history_fallback(self):
        self.client.get_messages.side_effect = RuntimeError("private error details")
        result = await self.run_request([1, 3])
        self.assertEqual(len(result["failed"]), 2)
        self.assertNotIn("private", str(result))
        self.client.get_messages.assert_awaited_once()

    async def test_transport_send_failure_never_retries(self):
        self.messages[1] = message(
            1, copy=AsyncMock(side_effect=TimeoutError("uncertain"))
        )
        result = await self.run_request([1, 2])
        self.messages[1].copy.assert_awaited_once()
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["failed"], [{"id": 1, "error": "TimeoutError"}])

    async def test_flood_wait_is_bounded_and_retryable(self):
        class Flood(Exception):
            ID, value = "FLOOD_WAIT", 1

        self.messages[1] = message(1, copy=AsyncMock(side_effect=[Flood(), object()]))
        with patch.object(self.api.asyncio, "sleep", AsyncMock()) as sleep:
            result = await self.run_request([1])
        self.assertEqual(result["sent"], 1)
        sleep.assert_awaited_once_with(1)
        self.messages[1].copy.side_effect = Flood()
        with patch.object(self.api.asyncio, "sleep", AsyncMock()) as sleep:
            result = await self.run_request([1])
        self.assertEqual(len(result["failed"]), 1)
        self.assertEqual(sleep.await_count, 2)

    async def test_cancel_does_not_read_next_message(self):
        self.messages[1] = message(
            1, copy=AsyncMock(side_effect=asyncio.CancelledError())
        )
        with self.assertRaises(asyncio.CancelledError):
            await self.run_request([1, 2], sequential=True)
        self.client.get_messages.assert_awaited_once()

    async def test_protected_selected_media_only_and_temp_cleanup(self):
        msg = message(
            1,
            media="video",
            video=types.SimpleNamespace(file_size=7),
            has_protected_content=True,
            media_group_id="do-not-expand",
        )
        self.messages[1] = msg
        paths = []

        async def download(selected, file_name):
            self.assertIs(selected, msg)
            Path(file_name).write_bytes(b"fixture")
            paths.append(Path(file_name))
            return file_name

        self.client.download_media.side_effect = download
        result = await self.run_request([1])
        self.assertEqual(result["sent"], 1)
        self.client.download_media.assert_awaited_once()
        self.client.send_video.assert_awaited_once_with(
            -1002,
            str(paths[0]),
            caption="fixture",
            caption_entities=[],
            parse_mode="disabled",
        )
        self.assertFalse(paths[0].exists())
        msg.copy.assert_not_awaited()

    async def test_protected_header_and_invalid_download_never_send(self):
        self.messages[1] = message(
            1,
            media="video",
            video=types.SimpleNamespace(file_size=7),
            has_protected_content=True,
        )
        result = await self.run_request([1], drop_author=False)
        self.assertEqual(len(result["failed"]), 1)
        self.client.download_media.assert_not_awaited()
        for downloaded in (None, "", "/nonexistent-fixture"):
            self.client.download_media.return_value = downloaded
            result = await self.run_request([1])
            self.assertEqual(len(result["failed"]), 1)
        self.client.send_video.assert_not_awaited()

    async def test_truncated_download_never_uploads(self):
        self.messages[1] = message(
            1,
            media="document",
            document=types.SimpleNamespace(file_size=99),
            has_protected_content=True,
        )

        async def download(selected, file_name):
            Path(file_name).write_bytes(b"partial")
            return file_name

        self.client.download_media.side_effect = download
        result = await self.run_request([1])
        self.assertEqual(len(result["failed"]), 1)
        self.client.send_document.assert_not_awaited()

    async def test_progress_counts_final_outcomes(self):
        progress = AsyncMock()
        result = await self.run_request([2, 1], on_progress=progress)
        self.assertEqual(progress.await_count, 2)
        self.assertEqual(progress.call_args.args[0], result)


if __name__ == "__main__":
    unittest.main()

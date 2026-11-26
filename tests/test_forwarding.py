"""Offline exact-ID API tests: deliberately no application or Telegram startup."""

import asyncio
import importlib.util
import socket
import types
import unittest
from copy import copy
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
        copy=AsyncMock(return_value=sent(mid)),
        forward=AsyncMock(return_value=sent(mid)),
        chat=types.SimpleNamespace(id=-1001),
        caption="fixture",
        caption_entities=[],
    )
    values.update(kwargs)
    return types.SimpleNamespace(**values)


def sent(mid):
    return types.SimpleNamespace(id=mid + 10000)


def media(mid, kind="photo", album="a", chat=-1001, **kwargs):
    return message(
        mid,
        media=kind,
        media_group_id=album,
        chat=types.SimpleNamespace(id=chat),
        **{kind: types.SimpleNamespace(file_id=f"cached_{mid}", file_size=4)},
        **kwargs,
    )


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
            send_video=AsyncMock(return_value=sent(1)),
            send_document=AsyncMock(return_value=sent(1)),
            send_media_group=AsyncMock(
                side_effect=lambda dst, group: [sent(i) for i in range(len(group))]
            ),
            forward_messages=AsyncMock(
                side_effect=lambda dst, src, ids: [sent(i) for i in ids]
            ),
        )

    async def run_request(self, ids=None, **kwargs):
        return await self.api.forward_messages(
            "@source",
            "@target",
            [123, 124, 126, 127] if ids is None else ids,
            client=self.client,
            **kwargs,
        )

    async def test_pause_completes_current_album_and_never_sends_tail(self):
        self.messages = {1:media(1),2:media(2),3:message(3),4:message(4)}
        pause = False
        async def after_group(result):
            nonlocal pause
            pause = True
        result = await self.run_request([1,2,3,4], on_progress=after_group, should_pause=lambda:pause)
        self.assertEqual(result['sent'],2)
        self.assertEqual(result['pending'],[3,4])
        self.assertEqual(result['missing'],[3,4])
        self.assertTrue(result['paused'])
        self.messages[3].copy.assert_not_awaited()
        self.messages[4].copy.assert_not_awaited()
        self.client.send_media_group.assert_awaited_once()

    async def test_pause_before_first_send_is_entirely_unsent(self):
        result = await self.run_request([1,2,3], should_pause=lambda:True)
        self.assertEqual(result['pending'],[1,2,3])
        self.assertEqual(result['sent'],0)
        self.client.send_media_group.assert_not_awaited()
        for item in self.messages.values():item.copy.assert_not_awaited()

    async def test_disk_preflight_fails_before_downloading_any_protected_media(self):
        item = media(1,kind='video',has_protected_content=True)
        with patch.object(self.api.shutil,'disk_usage',return_value=types.SimpleNamespace(free=0)):
            with self.assertRaisesRegex(OSError,'disk space'):
                await self.api.send_unit(self.client,[item],-1002,drop_author=True)
        self.client.download_media.assert_not_awaited()
        self.client.send_video.assert_not_awaited()

    async def test_exact_selection_and_metadata_only(self):
        result = await self.run_request()
        self.assertEqual({k: result[k] for k in ("requested", "sent", "missing", "failed")}, dict(requested=4, sent=4, missing=[], failed=[]))
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

    async def test_whole_dialog_explicit_snapshot_can_exceed_selection_limit(self):
        self.client.get_chat.side_effect = lambda peer, **kw: types.SimpleNamespace(
            id=peer, has_protected_content=False
        )
        with patch.object(self.api, "MAX_MESSAGES", 2):
            with self.assertRaises(ValueError):
                self.api.validate_request(-1001, -1002, [1, 2, 3])
            result = await self.api.forward_messages(
                -1001,
                -1002,
                [1, 2, 3],
                client=self.client,
                whole_dialog=True,
                sequential=True,
            )
        self.assertEqual(result["sent"], 3)
        self.client.get_messages.assert_awaited_once_with(
            -1001, message_ids=[1, 2, 3], replies=0
        )
        for source, ids, flag in (
            ("@source", [1, 2], True),
            (-1001, [2, 1], True),
            (-1001, [1, 1], True),
            (-1001, [1], 1),
        ):
            with self.assertRaises(ValueError):
                self.api.validate_request(source, -1002, ids, whole_dialog=flag)

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
            return sent(7)

        self.messages[7] = message(7, copy=copy)
        task = asyncio.create_task(
            self.run_request([7, 3], sequential=True, group_mode="split")
        )
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
            (0, "@target"),
            ("@source", 1),
            ("me", "@target"),
            ("@bad/user", "@target"),
            ("@source", "@SOURCE"),
            (-1, -1),
        ]:
            with self.assertRaises(ValueError):
                await self.api.forward_messages(src, dst, [1], client=self.client)
        with self.assertRaises(ValueError):
            await self.run_request([1], sequential=1)
        for options in (
            dict(group_mode=None),
            dict(group_mode="album"),
            dict(group_mode="force", drop_author=False),
        ):
            with self.assertRaises(ValueError):
                await self.run_request([1], **options)
        self.client.get_chat.assert_not_awaited()

    async def test_same_resolved_chat_fails_before_messages(self):
        self.client.get_chat.side_effect = None
        self.client.get_chat.return_value = types.SimpleNamespace(id=-1)
        with self.assertRaises(ValueError):
            await self.run_request()
        self.client.get_messages.assert_not_awaited()

    async def test_numeric_bot_source_is_supported_without_allowing_user_destinations(
        self,
    ):
        self.client.get_chat.side_effect = lambda peer, **kw: types.SimpleNamespace(
            id=peer, has_protected_content=False
        )
        self.messages[3] = message(3, chat=types.SimpleNamespace(id=880))
        result = await self.api.forward_messages(880, -1002, [3], client=self.client)
        self.assertEqual(result["sent"], 1)
        self.messages[3].copy.assert_awaited_once_with(-1002, reply_markup=None)
        with self.assertRaises(ValueError):
            self.api.validate_request(880, 990, [3])

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
        self.assertEqual([{k: f[k] for k in ("id", "error")} for f in result["failed"]], [{"id": 1, "error": "ValueError"}])
        extra.copy.assert_not_awaited()

    async def test_private_range_skips_other_dialogs_without_failing_valid_items(self):
        self.client.get_chat.side_effect = lambda peer, **kw: types.SimpleNamespace(
            id=880 if peer == "@source" else -1002, has_protected_content=False
        )
        for mid in range(1, 91):
            self.messages[mid] = message(
                mid,
                empty=29 <= mid <= 78,
                chat=types.SimpleNamespace(id=880 if mid <= 78 else 999),
            )
        result = await self.run_request(list(range(1, 91)))
        self.assertEqual(
            {k: v for k, v in result.items() if k != "receipts"},
            dict(
                requested=90,
                sent=28,
                missing=list(range(29, 91)),
                failed=[],
                skipped_other_chat=list(range(79, 91)),
            ),
        )
        self.client.get_messages.assert_awaited_once()
        for mid, item in self.messages.items():
            if mid <= 28:
                item.copy.assert_awaited_once()
            else:
                item.copy.assert_not_awaited()
                item.forward.assert_not_awaited()

    async def test_other_dialog_is_an_album_boundary_in_both_send_modes(self):
        for drop_author in (True, False):
            with self.subTest(drop_author=drop_author):
                self.messages = {1: media(1), 2: media(2, chat=999), 3: media(3)}
                result = await self.run_request([1, 2, 3], drop_author=drop_author)
                self.assertEqual(
                    {k: v for k, v in result.items() if k != "receipts"},
                    dict(
                        requested=3,
                        sent=2,
                        missing=[2],
                        failed=[],
                        skipped_other_chat=[2],
                    ),
                )
                self.messages[2].copy.assert_not_awaited()
                self.messages[2].forward.assert_not_awaited()
                self.client.send_media_group.assert_not_awaited()
                self.client.forward_messages.assert_not_awaited()

    async def test_duplicate_response_cannot_bypass_other_dialog_check(self):
        foreign, own = message(1, chat=types.SimpleNamespace(id=999)), message(1)
        self.client.get_messages.side_effect = None
        self.client.get_messages.return_value = [foreign, own]
        result = await self.run_request([1])
        self.assertEqual([{k: f[k] for k in ("id", "error")} for f in result["failed"]], [{"id": 1, "error": "ValueError"}])
        foreign.copy.assert_not_awaited()
        own.copy.assert_not_awaited()

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
        self.assertEqual([{k: f[k] for k in ("id", "error")} for f in result["failed"]], [{"id": 1, "error": "TimeoutError"}])

    async def test_flood_wait_is_bounded_and_retryable(self):
        class Flood(Exception):
            ID, value = "FLOOD_WAIT", 1

        self.messages[1] = message(1, copy=AsyncMock(side_effect=[Flood(), sent(1)]))
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
            await self.run_request([1, 2], sequential=True, group_mode="split")
        self.client.get_messages.assert_awaited_once()

    async def test_protected_selected_media_only_and_temp_cleanup(self):
        from pyrogram.enums import ParseMode

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
            self.assertIs(selected, msg.video)
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
            parse_mode=ParseMode.DISABLED,
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

    def test_keep_preserves_contiguous_boundaries_and_missing_barriers(self):
        messages = [
            media(7),
            media(3, "video"),
            None,
            media(6),
            media(2, album="b"),
            media(5),
            media(4, chat=-1009),
            media(1, chat=-1009),
            media(9, chat=None),
            media(10, album=None),
            media(12, album=None),
            message(80, empty=True),
            media(13),
            message(88, service=True),
            media(16),
        ]
        units = self.api.plan_units(messages)
        self.assertEqual(
            [[m.id for m in u] for u in units],
            [[7, 3], [6], [2], [5], [4, 1], [9], [10], [12], [13], [16]],
        )
        self.assertEqual(
            [len(u) for u in self.api.plan_units(messages, "split")], [1] * 12
        )

    def test_force_chunks_types_sources_and_unknown_kinds(self):
        selected = [
            media(i, "video" if i % 2 else "photo", album=None) for i in range(1, 12)
        ]
        selected += [
            media(12, "audio"),
            media(13, "audio"),
            media(14, "document"),
            media(15, "document"),
            media(16, "voice"),
            media(17, "voice"),
            media(18, "photo"),
            media(19, "photo", chat=55),
            message(20),
        ]
        self.assertEqual(
            [self.api.media_kind(m) for m in selected[-5:]],
            ["voice", "voice", "photo", "photo", None],
        )
        units = self.api.plan_units(selected, "force")
        self.assertEqual([len(u) for u in units], [10, 1, 2, 2, 1, 1, 1, 1, 1])
        self.assertEqual([m.id for u in units for m in u], list(range(1, 21)))
        with self.assertRaises(ValueError):
            self.api.plan_units([], "merge")

    async def test_keep_header_only_exact_selected_ids_in_request_order(self):
        self.client.get_messages.side_effect = None
        self.client.get_messages.return_value = [media(2), media(91), media(7)]
        result = await self.run_request([7, 91, 45, 2], drop_author=False)
        self.assertEqual({k: result[k] for k in ("requested", "sent", "missing", "failed")}, dict(requested=4, sent=3, missing=[45], failed=[]))
        self.client.forward_messages.assert_awaited_once_with(-1002, -1001, [7, 91])
        self.client.get_messages.return_value[0].forward.assert_awaited_once_with(-1002)
        self.client.get_messages.assert_awaited_once_with(
            -1001, message_ids=[7, 91, 45, 2], replies=0
        )
        self.client.download_media.assert_not_awaited()

    async def test_keep_across_fetch_boundary_without_reordering(self):
        ids = list(range(205, 1, -2))
        lookup = {
            mid: media(mid, album=("a" if i >= 98 else "b"))
            for i, mid in enumerate(ids)
        }
        self.client.get_messages.side_effect = lambda peer, message_ids, replies: [
            lookup[mid] for mid in reversed(message_ids)
        ]
        result = await self.run_request(ids, sequential=True, drop_author=False)
        self.assertEqual(result["sent"], len(ids))
        groups = [c.args[2] for c in self.client.forward_messages.await_args_list]
        self.assertEqual(groups[-1], ids[98:])
        self.assertEqual([mid for group in groups for mid in group], ids)
        self.assertEqual(
            [c.kwargs["message_ids"] for c in self.client.get_messages.await_args_list],
            [ids[:100], ids[100:]],
        )

    async def test_failed_fetch_is_a_boundary(self):
        self.client.get_messages.side_effect = [
            [media(100)],
            RuntimeError("offline fixture"),
            [media(201)],
        ]
        result = await self.run_request(list(range(1, 202)), group_mode="force")
        self.assertEqual(result["sent"], 2)
        self.assertEqual(result["missing"], list(range(1, 100)))
        self.assertEqual([f["id"] for f in result["failed"]], list(range(101, 201)))
        self.client.send_media_group.assert_not_awaited()

    async def test_sequential_group_waits_before_next_send_unit(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def group(dst, items):
            entered.set()
            await release.wait()
            return [sent(i) for i in range(len(items))]

        self.client.send_media_group.side_effect = group
        self.messages.update({i: media(i) for i in (1, 2)})
        third = self.messages[3] = message(3)
        task = asyncio.create_task(self.run_request([1, 2, 3], sequential=True))
        await asyncio.wait_for(entered.wait(), 3)
        third.copy.assert_not_awaited()
        release.set()
        self.assertEqual((await task)["sent"], 3)
        third.copy.assert_awaited_once()

    async def test_direct_group_uses_supplied_metadata_not_refetch(self):
        from pyrogram import enums
        from pyrogram import types as tg

        first = media(9, has_media_spoiler=True, caption="old")
        edited = copy(first)
        entity = tg.MessageEntity(type=enums.MessageEntityType.BOLD, offset=2, length=7)
        edited.caption, edited.caption_entities = "  **literal** 🪁 ", [entity]
        second = media(3, "video", caption="next")
        second.video.duration, second.video.width = 99, 300
        results = await self.api.send_unit(self.client, [edited, second], -1002)
        self.assertEqual(len(results), 2)
        dst, payload = self.client.send_media_group.await_args.args
        self.assertEqual(dst, -1002)
        self.assertIsInstance(payload[0], tg.InputMediaPhoto)
        self.assertIsInstance(payload[1], tg.InputMediaVideo)
        self.assertEqual([p.media for p in payload], ["cached_9", "cached_3"])
        self.assertEqual([p.caption for p in payload], [edited.caption, "next"])
        self.assertEqual(payload[0].caption_entities, [entity])
        self.assertEqual(payload[0].parse_mode, enums.ParseMode.DISABLED)
        self.assertTrue(payload[0].has_spoiler)
        self.assertEqual((payload[1].duration, payload[1].width), (99, 300))
        self.client.get_messages.assert_not_awaited()
        self.client.download_media.assert_not_awaited()

    async def test_real_sdk_serializes_exact_caption_entities_without_transport(self):
        from pyrogram import Client, enums, raw
        from pyrogram import types as tg
        from pyrogram.parser import Parser

        # No constructor, config, storage, session or network. Stop at the raw request.
        sdk = object.__new__(Client)
        sdk.parser, sdk.rnd_id = Parser(None), lambda: 1
        sdk.resolve_peer = AsyncMock(return_value=raw.types.InputPeerSelf())
        sdk.invoke = AsyncMock(
            return_value=types.SimpleNamespace(updates=[], users=[], chats=[])
        )
        entity = tg.MessageEntity(type=enums.MessageEntityType.BOLD, offset=3, length=2)
        one = media(7, caption=" 🪁hi ", caption_entities=[entity])
        two = media(8, caption="  **hi** ")
        with patch(
            "pyrogram.utils.get_input_media_from_file_id",
            return_value=raw.types.InputMediaEmpty(),
        ), patch(
            "pyrogram.utils.parse_messages", AsyncMock(return_value=[sent(7), sent(8)])
        ):
            await self.api.send_unit(sdk, [one, two], -1002)
        request = sdk.invoke.await_args.args[0]
        self.assertIsInstance(request, raw.functions.messages.SendMultiMedia)
        self.assertEqual(
            [p.message for p in request.multi_media], [one.caption, two.caption]
        )
        self.assertEqual(sdk.invoke.await_args.kwargs["retries"], 0)
        self.assertEqual(sdk.invoke.await_args.kwargs["sleep_threshold"], 0)
        raw_entity = request.multi_media[0].entities[0]
        self.assertIsInstance(raw_entity, raw.types.MessageEntityBold)
        self.assertEqual((raw_entity.offset, raw_entity.length), (3, 2))
        self.assertIsNone(entity._client)
        self.assertIsNone(request.multi_media[1].entities)

        class AfterSend(Exception):
            ID, value = "FLOOD_WAIT", 1

        sdk.invoke.reset_mock()
        with patch(
            "pyrogram.utils.get_input_media_from_file_id",
            return_value=raw.types.InputMediaEmpty(),
        ), patch(
            "pyrogram.utils.parse_messages", AsyncMock(side_effect=AfterSend())
        ), self.assertRaises(
            AfterSend
        ):
            await self.api.send_unit(sdk, [one, two], -1002)
        sdk.invoke.assert_awaited_once()

    async def test_real_sdk_copy_uses_frontend_caption_without_mutation(self):
        from pyrogram import Client, enums, raw
        from pyrogram import types as tg
        from pyrogram.parser import Parser

        sdk = object.__new__(Client)
        sdk.parser, sdk.rnd_id = Parser(None), lambda: 1
        sdk.resolve_peer = AsyncMock(return_value=raw.types.InputPeerSelf())
        update = raw.types.UpdateNewMessage(
            message=raw.types.MessageEmpty(id=909), pts=1, pts_count=1
        )
        sdk.invoke = AsyncMock(
            return_value=types.SimpleNamespace(updates=[update], users=[], chats=[])
        )
        original = tg.Message(
            id=7,
            media=enums.MessageMediaType.PHOTO,
            photo=media(7).photo,
            caption="original",
            reply_markup=object(),
            client=sdk,
        )
        entity = tg.MessageEntity(
            type=enums.MessageEntityType.ITALIC, offset=1, length=2
        )
        for text, entities in [(" 🪁 ", [entity]), ("", [])]:
            edited = copy(original)
            edited.caption, edited.caption_entities = text, entities
            with patch(
                "pyrogram.utils.get_input_media_from_file_id",
                return_value=raw.types.InputMediaEmpty(),
            ), patch.object(tg.Message, "_parse", AsyncMock(return_value=sent(909))):
                await self.api.send_unit(sdk, [edited], -1002)
            query = sdk.invoke.await_args.args[0]
            self.assertEqual(query.message, text)
            self.assertIsNone(query.reply_markup)
            self.assertEqual(len(query.entities or []), len(entities))
            self.assertIsNone(entity._client)
            self.assertIsNone(getattr(edited, "_client", None))
        self.assertEqual(original.caption, "original")
        self.assertIs(original._client, sdk)
        self.assertNotIn("parse", vars(sdk.parser))

    async def test_real_sdk_pin_parser_cannot_read_an_unselected_message(self):
        from pyrogram import Client, raw
        from pyrogram.parser import Parser

        sdk = object.__new__(Client)
        sdk.parser = Parser(None)
        sdk.get_chat = self.client.get_chat
        sdk.resolve_peer = AsyncMock(return_value=raw.types.InputPeerChat(chat_id=1001))
        sdk.invoke = AsyncMock(return_value=object())

        async def parse_response(scoped, response, replies):
            self.assertEqual(replies, 0)
            # The installed Pyrogram parser makes this lookup for a pin service action.
            related = await scoped.get_messages(
                -1001, reply_to_message_ids=7, replies=0
            )
            self.assertIsNone(related)
            return [message(7, service=True)]

        with patch("pyrogram.utils.parse_messages", parse_response):
            result = await self.api.forward_messages(
                "@source", "@target", [7], client=sdk
            )
        self.assertEqual({k: result[k] for k in ("requested", "sent", "missing", "failed")}, dict(requested=1, missing=[7], failed=[], sent=0))
        sdk.invoke.assert_awaited_once()
        ids = sdk.invoke.await_args.args[0].id
        self.assertEqual([item.id for item in ids], [7])
        self.assertIsInstance(ids[0], raw.types.InputMessageID)

    async def test_bad_unit_no_send_and_no_fetch(self):
        for items, header in [
            ([], False),
            ([media(1), None], False),
            ([media(1), media(1)], False),
            ([media(1), media(2, "audio")], False),
            ([media(1), media(2, chat=55)], False),
            ([media(1), media(2, album="b")], True),
            ([media(1, has_protected_content=True), media(2)], True),
            ([media(i) for i in range(1, 12)], False),
        ]:
            with self.assertRaises(ValueError):
                await self.api.send_unit(
                    self.client, items, -1002, drop_author=not header
                )
        self.client.send_media_group.assert_not_awaited()
        self.client.forward_messages.assert_not_awaited()
        self.client.get_messages.assert_not_awaited()
        self.client.download_media.assert_not_awaited()

    async def test_protected_group_files_live_through_send_and_cancel(self):
        selected = [media(7, "document"), media(1, "document")]
        for m in selected:
            m.document.file_name = "../../same.bin"
        paths = []

        async def download(item, file_name):
            self.assertIs(item, selected[len(paths)].document)
            path = Path(file_name)
            path.write_bytes(b"data")
            paths.append(path)
            return file_name

        async def upload(dst, group):
            self.assertEqual([Path(item.media) for item in group], paths)
            self.assertEqual([p.read_bytes() for p in paths], [b"data"] * 2)
            self.assertNotEqual(paths[0].parent, paths[1].parent)
            self.assertEqual([p.name for p in paths], ["same.bin"] * 2)
            raise asyncio.CancelledError()

        self.client.download_media.side_effect = download
        self.client.send_media_group.side_effect = upload
        with self.assertRaises(asyncio.CancelledError):
            await self.api.send_unit(self.client, selected, -1002, protected=True)
        self.assertTrue(all(not p.parent.exists() for p in paths))
        self.client.send_media_group.assert_awaited_once()

    async def test_mixed_protection_downloads_only_protected_member(self):
        first, second = media(4, "audio", has_protected_content=True), media(9, "audio")
        first.audio.performer, first.audio.title = "performer", "title"

        async def download(item, file_name):
            self.assertIs(item, first.audio)
            Path(file_name).write_bytes(b"data")
            return file_name

        self.client.download_media.side_effect = download
        await self.api.send_unit(self.client, [first, second], -1002)
        self.client.download_media.assert_awaited_once()
        group = self.client.send_media_group.await_args.args[1]
        self.assertFalse(Path(group[0].media).exists())
        self.assertEqual(
            (group[0].performer, group[0].title, group[1].media),
            ("performer", "title", "cached_9"),
        )

    async def test_cancel_during_second_download_cleans_both_paths(self):
        entered = asyncio.Event()
        paths = []

        async def download(item, file_name):
            path = Path(file_name)
            path.write_bytes(b"data")
            paths.append(path)
            if len(paths) == 2:
                entered.set()
                await asyncio.Event().wait()
            return file_name

        self.client.download_media.side_effect = download
        task = asyncio.create_task(
            self.api.send_unit(self.client, [media(7), media(1)], -1002, protected=True)
        )
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertFalse(any(p.parent.exists() for p in paths))
        self.client.send_media_group.assert_not_awaited()

    async def test_group_uncertain_never_retries_or_falls_back(self):
        self.messages.update({i: media(i) for i in (1, 2)})
        for response in (
            [],
            [sent(1)],
            [sent(1)] * 2,
            [sent(1), None],
            [sent(1), types.SimpleNamespace(id=True)],
            [sent(1), types.SimpleNamespace(id=0)],
            [sent(1), sent(2), sent(3)],
            None,
            TimeoutError(),
        ):
            self.client.send_media_group.reset_mock()
            self.client.send_media_group.side_effect = (
                response if isinstance(response, Exception) else None
            )
            self.client.send_media_group.return_value = response
            result = await self.run_request([2, 1])
            self.assertEqual(result["sent"], 0)
            self.assertEqual([f["id"] for f in result["failed"]], [2, 1])
            self.assertEqual(
                result["failed"][0]["error"],
                "TimeoutError"
                if isinstance(response, Exception)
                else "UnconfirmedSendError",
            )
            self.client.send_media_group.assert_awaited_once()
        for item in self.messages.values():
            item.copy.assert_not_awaited()

    async def test_group_floodwait_only_bounded_retries(self):
        class Flood(Exception):
            ID, value = "FLOOD_PREMIUM_WAIT", 2

        group = [media(1), media(2)]
        self.client.send_media_group.side_effect = [Flood(), [sent(1), sent(2)]]
        with patch.object(self.api.asyncio, "sleep", AsyncMock()) as sleep:
            self.assertEqual(
                len(await self.api.send_unit(self.client, group, -1002)), 2
            )
        sleep.assert_awaited_once_with(2)
        self.client.send_media_group.reset_mock()
        self.client.send_media_group.side_effect = Flood()
        with patch.object(
            self.api.asyncio, "sleep", AsyncMock()
        ) as sleep, self.assertRaises(Flood):
            await self.api.send_unit(self.client, group, -1002)
        self.assertEqual(self.client.send_media_group.await_count, 3)
        self.assertEqual(sleep.await_count, 2)


class PhotoFilenameTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_sdk_uploads_downloaded_album_photos_with_jpeg_names(self):
        from pyrogram import Client, raw
        from pyrogram.parser import Parser

        api = load_executor()
        sdk = object.__new__(Client)
        sdk.parser, sdk.rnd_id = Parser(None), lambda: 1
        sdk.resolve_peer = AsyncMock(return_value=raw.types.InputPeerSelf())
        filenames, uploads, paths = [], [], []

        async def download(selected, file_name, **kwargs):
            paths.append(Path(file_name))
            Path(file_name).write_bytes(b'jpeg')
            await kwargs['progress'](4, 4)
            return file_name

        async def save_file(path, **kwargs):
            name = Path(path).name
            filenames.append(name)
            await kwargs['progress'](4, 4)
            return raw.types.InputFile(id=1, parts=1, name=name, md5_checksum='')

        async def invoke(request, **kwargs):
            if isinstance(request, raw.functions.messages.UploadMedia):
                uploads.append(request)
                # Telegram rejects an uploaded photo with an extensionless filename.
                self.assertEqual(Path(request.media.file.name).suffix, '.jpg')
                return types.SimpleNamespace(photo=types.SimpleNamespace(id=len(uploads),access_hash=2,file_reference=b'fixture'))
            self.assertIsInstance(request, raw.functions.messages.SendMultiMedia)
            return types.SimpleNamespace(updates=[],users=[],chats=[])

        sdk.download_media = AsyncMock(side_effect=download)
        sdk.save_file = AsyncMock(side_effect=save_file)
        sdk.invoke = AsyncMock(side_effect=invoke)
        status = AsyncMock()
        with patch('pyrogram.utils.parse_messages', AsyncMock(return_value=[sent(7),sent(8)])):
            result = await api.send_unit(sdk,[media(7),media(8)],-1002,protected=True,on_status=status)
        self.assertEqual(len(result),2)
        self.assertEqual(filenames,['photo.jpg','photo.jpg'])
        self.assertEqual(len(uploads),2)
        self.assertNotEqual(paths[0].parent,paths[1].parent)
        self.assertTrue(all(not path.exists() for path in paths))
        events = [call.args[0] for call in status.call_args_list]
        self.assertTrue({'download', 'upload', 'confirm'} <= {e['stage'] for e in events})
        self.assertTrue(all(e['unit_size'] == 2 for e in events))
        self.assertEqual({e['file_index'] for e in events if e['stage'] == 'download'}, {1,2})
        self.assertEqual({e['file_index'] for e in events if e['stage'] == 'upload'}, {1,2})

    async def test_photo_metadata_cannot_remove_the_upload_extension(self):
        import tempfile
        api=load_executor()
        client=types.SimpleNamespace()
        async def download(selected,file_name):
            Path(file_name).write_bytes(b'jpeg')
            return file_name
        client.download_media=AsyncMock(side_effect=download)
        for name in (None,'media','../../unsafe.bin','bad.exe'):
            with self.subTest(name=name),tempfile.TemporaryDirectory() as folder:
                item=media(7)
                item.photo.file_name=name
                path=await api._download(client,item,'photo',folder)
                self.assertEqual(Path(path).name,'photo.jpg')
                self.assertEqual(Path(path).parent,Path(folder))


if __name__ == "__main__":
    unittest.main()

"""Transfer telemetry must preserve exactly-once sends and the cached-media fast path."""
import asyncio
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from tests.test_forwarding import load_executor, media, sent


class TransferProgressTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.api = load_executor()
        self.events = []

    async def record(self, event):
        self.events.append(event)

    async def test_large_unprotected_video_never_downloads_or_uploads(self):
        client = types.SimpleNamespace(download_media=AsyncMock(), send_video=AsyncMock())
        item = media(1, kind='video', album=None)
        item.video.file_size = 923469536
        result = await self.api.send_unit(client, [item], -1002, on_status=self.record)
        self.assertEqual(result, [sent(1)])
        client.download_media.assert_not_awaited()
        client.send_video.assert_not_awaited()
        item.copy.assert_awaited_once()
        self.assertEqual(self.events[0]['stage'], 'copy')

    async def test_missing_sdk_total_uses_source_size_and_clamps_chunks(self):
        item = media(1)
        async def download(selected, file_name, progress):
            await progress(512 * 1024, 0)
            Path(file_name).write_bytes(b'data')
            return file_name
        progress = self.api._TransferProgress(self.record)
        with tempfile.TemporaryDirectory() as folder:
            await self.api._download(types.SimpleNamespace(download_media=download), item, 'photo', folder, progress)
            await progress.emit(force=True)
        self.assertEqual(self.events[-1]['current'], 4)
        self.assertEqual(self.events[-1]['total'], 4)

    async def test_broken_telemetry_never_changes_confirmed_send(self):
        item = media(1, album=None)
        result = await self.api.send_unit(types.SimpleNamespace(), [item], -1002,
            on_status=AsyncMock(side_effect=RuntimeError('display failed')))
        self.assertEqual(result, [sent(1)])
        item.copy.assert_awaited_once()

    async def test_real_sdk_flood_wait_ids_retry_the_same_rpc(self):
        from pyrogram import Client, errors, raw

        for error_type in (errors.FloodWait, errors.FloodPremiumWait):
            with self.subTest(error_type=error_type.__name__):
                client = Client("offline-flood", api_id=1, api_hash="fixture", in_memory=True)
                client.invoke = AsyncMock(side_effect=[error_type(4), "accepted"])
                scoped = self.api._unit_client(client)
                request = raw.functions.messages.SendMessage(
                    peer=raw.types.InputPeerSelf(), message="offline", random_id=42
                )
                with patch.object(self.api.asyncio, "sleep", AsyncMock()) as sleep:
                    self.assertEqual(await scoped.invoke(request), "accepted")
                sleep.assert_awaited_once_with(4)
                self.assertEqual(client.invoke.await_count, 2)
                for call in client.invoke.await_args_list:
                    self.assertIs(call.args[0], request)
                    self.assertEqual(call.kwargs, {"retries": 0, "sleep_threshold": 0})

    async def test_real_sdk_flood_wait_respects_existing_limits(self):
        from pyrogram import errors

        too_long = AsyncMock(side_effect=errors.FloodWait(self.api.MAX_FLOOD_WAIT_SECONDS + 1))
        repeated = AsyncMock(side_effect=errors.FloodWait(1))
        with patch.object(self.api.asyncio, "sleep", AsyncMock()) as sleep:
            with self.assertRaises(errors.FloodWait):
                await self.api._flood_retry(too_long)
            too_long.assert_awaited_once()
            sleep.assert_not_awaited()
            with self.assertRaises(errors.FloodWait):
                await self.api._flood_retry(repeated)
            self.assertEqual(repeated.await_count, 3)
            self.assertEqual(sleep.await_count, 2)

    async def test_flood_wait_restores_phase_without_retrying_other_errors(self):
        class Flood(Exception):
            ID, value = 'FLOOD_WAIT', 2
        progress = self.api._TransferProgress(self.record)
        await progress.stage('copy')
        operation = AsyncMock(side_effect=[Flood(), 'ok'])
        with patch.object(self.api.asyncio, 'sleep', AsyncMock()) as sleep:
            self.assertEqual(await self.api._flood_retry(operation, progress), 'ok')
        sleep.assert_awaited_once_with(2)
        self.assertEqual([v['stage'] for v in self.events], ['copy', 'flood_wait', 'copy'])
        operation = AsyncMock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):
            await self.api._flood_retry(operation, progress)
        operation.assert_awaited_once()

    async def test_cancel_during_wait_propagates_without_resend(self):
        class Flood(Exception):
            ID, value = 'FLOOD_WAIT', 2
        operation = AsyncMock(side_effect=Flood())
        with patch.object(self.api.asyncio, 'sleep', AsyncMock(side_effect=asyncio.CancelledError())), self.assertRaises(asyncio.CancelledError):
            await self.api._flood_retry(operation, self.api._TransferProgress(self.record))
        operation.assert_awaited_once()

    async def test_actual_sdk_album_save_file_hook_is_scoped(self):
        from pyrogram import Client
        from pyrogram.parser import Parser
        sdk = object.__new__(Client)
        sdk.parser = Parser(None)
        progress = self.api._TransferProgress(self.record)
        async def save(instance, path, **kwargs):
            if path is not None:
                await kwargs['progress'](4, 4)
            return 'uploaded' if path is not None else None
        with patch.object(Client, 'save_file', save):
            scoped = self.api._unit_client(sdk, progress)
            with tempfile.NamedTemporaryFile() as stream:
                stream.write(b'data'); stream.flush()
                self.assertEqual(await scoped.save_file(stream.name), 'uploaded')
                self.assertIsNone(await scoped.save_file(None))
        self.assertNotIn('save_file', vars(sdk))
        self.assertEqual([e['stage'] for e in self.events], ['upload', 'confirm'])

    async def test_sdk_parser_error_after_send_is_never_replayed_with_progress(self):
        from pyrogram import Client, raw
        from pyrogram.parser import Parser
        sdk = object.__new__(Client)
        sdk.parser, sdk.rnd_id = Parser(None), lambda: 1
        sdk.resolve_peer = AsyncMock(return_value=raw.types.InputPeerSelf())
        sdk.invoke = AsyncMock(return_value=types.SimpleNamespace(updates=[], users=[], chats=[]))
        class ParseFlood(Exception):
            ID, value = 'FLOOD_WAIT', 1
        with patch('pyrogram.utils.get_input_media_from_file_id', return_value=raw.types.InputMediaEmpty()), \
             patch('pyrogram.utils.parse_messages', AsyncMock(side_effect=ParseFlood())), \
             self.assertRaises(ParseFlood):
            await self.api.send_unit(sdk, [media(1), media(2)], -1002, on_status=self.record)
        sdk.invoke.assert_awaited_once()

    async def test_progress_reset_on_restarted_transfer(self):
        progress = self.api._TransferProgress(self.record)
        await progress.stage('download', current=0, total=100)
        await progress.bytes(90, 0)
        progress.started -= 30
        await progress.bytes(10, 0)
        await progress.emit(force=True)
        self.assertLess(self.events[-1]['elapsed'], 1)
        self.assertEqual(self.events[-1]['current'], 10)

    async def test_managed_long_wait_retries_original_sdk_request(self):
        from pyrogram import Client, errors, raw

        client = Client('offline-managed', api_id=1, api_hash='fixture', in_memory=True)
        client.invoke = AsyncMock(side_effect=[errors.FloodWait(90), 'accepted'])
        flood = self.api._FloodControl(100, None)
        progress = self.api._TransferProgress(self.record, flood=flood)
        await progress.stage('copy')
        scoped = self.api._unit_client(client, progress)
        request = raw.functions.messages.SendMessage(peer=raw.types.InputPeerSelf(),message='fixture',random_id=42)
        with patch.object(self.api.asyncio,'sleep',AsyncMock()) as sleep:
            self.assertEqual(await scoped.invoke(request), 'accepted')
        sleep.assert_awaited_once_with(90)
        self.assertTrue(all(call.args[0] is request for call in client.invoke.await_args_list))
        self.assertEqual(client.invoke.await_count, 2)
        self.assertEqual(flood.remaining, 10)
        self.assertEqual([event['stage'] for event in self.events], ['copy','flood_wait','copy'])

    async def test_managed_exhaustion_or_pause_never_attempts_later_units(self):
        from pyrogram import errors

        for budget, pause_in_wait in ((60,False),(100,True)):
            with self.subTest(pause_in_wait=pause_in_wait):
                paused = False
                async def sleep(_):
                    nonlocal paused
                    paused = pause_in_wait
                one, two = media(1,album=None), media(2,album=None)
                client = types.SimpleNamespace(
                    get_chat=AsyncMock(side_effect=[types.SimpleNamespace(id=-1001),types.SimpleNamespace(id=-1002)]),
                    get_messages=AsyncMock(return_value=[one,two]))
                operation = AsyncMock(side_effect=errors.FloodWait(90))
                async def send(client,unit,dst,*,flood,**opts):
                    progress = self.api._TransferProgress(self.record, flood=flood)
                    await self.api._flood_retry(operation,progress,safe_to_resume=True)
                with patch.object(self.api,'send_unit',AsyncMock(side_effect=send)) as send_unit, \
                     patch.object(self.api.asyncio,'sleep',AsyncMock(side_effect=sleep)):
                    result=await self.api.forward_messages(-1001,-1002,[1,2],client=client,
                        flood_wait_budget=budget,should_pause=lambda:paused)
                send_unit.assert_awaited_once()
                operation.assert_awaited_once()
                self.assertTrue(result['paused'])
                self.assertEqual(result['pending'],[1,2])
                self.assertGreater(result['flood_retry_at'],0)

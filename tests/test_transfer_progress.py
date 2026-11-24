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

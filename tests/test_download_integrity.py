"""Isolate production functions; never import application config or use Telegram."""
import ast
import asyncio
import os
import socket
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import pyrogram
from pyrogram import Client, raw

ROOT = Path(__file__).resolve().parents[1]


class SizeMismatch(Exception):
    pass


def load_functions(path, names, namespace):
    tree = ast.parse(path.read_text())
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert {node.name for node in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    future = ast.parse("from __future__ import annotations").body
    exec(compile(ast.fix_missing_locations(ast.Module(body=future + nodes, type_ignores=[])),
                 str(path), "exec"), namespace)


class DownloaderIntegrityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="download-integrity-")
        self.addCleanup(self.temp.cleanup)
        self.final = Path(self.temp.name, "saved.bin")
        self.partial = Path(self.temp.name, "partial.bin")
        self.media = types.SimpleNamespace(file_id="selected", file_size=100)
        self.message = types.SimpleNamespace(id=7, document=self.media,
            reply_to_story=types.SimpleNamespace(document=types.SimpleNamespace(file_id="story")))
        self.status = types.SimpleNamespace(SkipDownload="skip", SuccessDownload="success", FailedDownload="failed")
        self.env = dict(os=os, time=time, pyrogram=pyrogram, logger=Mock(), _t=lambda s: s,
            asyncio=types.SimpleNamespace(sleep=AsyncMock()), app=types.SimpleNamespace(hide_file_name=False),
            DownloadStatus=self.status, MediaSizeMismatchError=SizeMismatch, RETRY_TIME_OUT=0,
            fetch_message=AsyncMock(return_value=self.message),
            _get_media_meta=AsyncMock(return_value=(str(self.final), str(self.partial), "bin")),
            _can_download=lambda *args: True, _is_exist=os.path.isfile,
            update_download_status=Mock(), _move_to_download_path=os.replace, _check_timeout=lambda *args: False)
        load_functions(ROOT / "media_downloader.py", ["_download_size_matches", "_check_download_finish",
                       "download_media", "download_chat_task", "download_chat_task_sequential"], self.env)
        self.client = types.SimpleNamespace(download_media=AsyncMock(side_effect=self.download))
        self.node = types.SimpleNamespace(chat_id=42)

    async def download(self, selected, **kwargs):
        self.assertIs(selected, self.media)
        self.partial.write_bytes(b"x" * selected.file_size)
        return str(self.partial)

    async def run_download(self):
        return await self.env["download_media"](self.client, self.message, ["document"], {}, self.node)

    async def test_existing_file_requires_exact_positive_size(self):
        for size in (0, 4, 100, 101):
            with self.subTest(size=size):
                self.final.write_bytes(b"x" * size)
                self.client.download_media.reset_mock()
                status, path = await self.run_download()
                self.assertEqual(status, "skip" if size == 100 else "success")
                self.assertEqual(self.client.download_media.await_count, 0 if size == 100 else 1)
                self.assertEqual(Path(path).stat().st_size, 100)
        for unknown in (0, None, True, -1):
            self.assertFalse(self.env["_download_size_matches"](100, unknown))

    async def test_progress_retains_known_size_with_media_object(self):
        self.env["update_download_status"] = AsyncMock()
        async def download(selected, **kwargs):
            callback, args = kwargs["progress"], kwargs["progress_args"]
            await callback(40, 0, *args)
            await callback(1024, 0, *args)
            self.partial.write_bytes(b"x" * 100)
            return str(self.partial)
        self.client.download_media.side_effect = download
        self.assertEqual((await self.run_download())[0], "success")
        calls = self.env["update_download_status"].await_args_list
        self.assertEqual([call.args[:2] for call in calls], [(40, 100), (100, 100)])
        self.assertEqual(calls[0].args[2], self.message.id)

    async def test_failed_redownload_preserves_existing_file(self):
        self.final.write_bytes(b"keep")
        async def truncated(*args, **kwargs):
            self.partial.write_bytes(b"bad")
            return str(self.partial)
        self.client.download_media.side_effect = truncated
        result = await self.run_download()
        self.assertEqual(result, ("failed", None))
        self.assertEqual(self.final.read_bytes(), b"keep")
        self.assertFalse(self.partial.exists())
        self.assertEqual(self.client.download_media.await_count, 3)

    def test_empty_download_is_not_a_complete_unknown_size_file(self):
        self.partial.write_bytes(b"")
        with self.assertRaises(SizeMismatch):
            self.env["_check_download_finish"](0, str(self.partial), "fixture")
        self.assertFalse(self.partial.exists())

    async def test_retry_uses_refreshed_selected_media_not_story(self):
        refreshed = types.SimpleNamespace(file_id="refreshed", file_size=100)
        second = types.SimpleNamespace(id=7, document=refreshed, reply_to_story=self.message.reply_to_story)
        self.env["fetch_message"].side_effect = [self.message, second]
        seen = []
        async def download(selected, **kwargs):
            seen.append(selected)
            if len(seen) == 1:
                raise pyrogram.errors.FileReferenceExpired()
            self.partial.write_bytes(b"x" * 100)
            return str(self.partial)
        self.client.download_media.side_effect = download
        self.assertEqual((await self.run_download())[0], "success")
        self.assertEqual(seen, [self.media, refreshed])

    def story_client(self):
        client = Client("offline", api_id=1, api_hash="fixture", in_memory=True, no_updates=True)
        client.resolve_peer = AsyncMock(return_value=raw.types.InputPeerUser(user_id=42, access_hash=1))
        client.invoke = AsyncMock(return_value=raw.types.messages.Messages(
            messages=[raw.types.Message(id=7, peer_id=raw.types.PeerUser(user_id=42), date=1,
                message="fixture", entities=[], reply_to=raw.types.MessageReplyStoryHeader(
                    peer=raw.types.PeerUser(user_id=42), story_id=9))],
            users=[raw.types.User(id=42, first_name="Fixture", usernames=[], restriction_reason=[])], chats=[]))
        client.get_stories = AsyncMock(side_effect=AssertionError("story fetch forbidden"))
        return client

    async def test_fetch_message_handles_story_reply_without_expansion(self):
        namespace = {}
        load_functions(ROOT / "module/pyrogram_extension.py", ["fetch_message"], namespace)
        client = self.story_client()
        message = types.SimpleNamespace(id=7, chat=types.SimpleNamespace(id=42))
        with patch.object(socket.socket, "connect", side_effect=AssertionError("network")):
            result = await namespace["fetch_message"](client, message)
        self.assertEqual(result.id, 7)
        self.assertEqual(result.reply_to_story_id, 9)
        client.get_stories.assert_not_awaited()
        client.invoke.assert_awaited_once()

    async def test_both_retry_queues_handle_story_reply_without_expansion(self):
        async def empty_history(*args, **kwargs):
            if False:
                yield None
        self.env["get_chat_history_v2"] = empty_history
        self.env["_process_chat_message_for_download"] = AsyncMock()
        for name in ("download_chat_task", "download_chat_task_sequential"):
            with self.subTest(entry=name):
                client = self.story_client()
                config = types.SimpleNamespace(ids_to_retry=[7], last_read_message_id=0)
                node = types.SimpleNamespace(chat_id=42, limit=1, end_offset_id=0,
                                             is_stop_transmission=False, total_task=0)
                self.env["_process_chat_message_for_download"].reset_mock()
                with patch.object(socket.socket, "connect", side_effect=AssertionError("network")):
                    await self.env[name](client, config, node)
                self.env["_process_chat_message_for_download"].assert_awaited_once()
                client.get_stories.assert_not_awaited()
                client.invoke.assert_awaited_once()

"""Offline regressions for upload cleanup, cancellation and failure accounting."""
import ast
import inspect
import asyncio
from collections import defaultdict
from enum import Enum
from functools import wraps
import os
from pathlib import Path
import secrets
import tempfile
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, Mock
import pyrogram

ROOT = Path(__file__).resolve().parents[1]

def load(path, names, env):
    tree = ast.parse((ROOT / path).read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.name in names]
    assert {n.name for n in nodes} == set(names)
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=ast.parse('from __future__ import annotations').body + nodes,
        type_ignores=[])), str(ROOT / path), 'exec'), env)

class PipelineSafetyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = dict(Enum=Enum, os=os, time=time, secrets=secrets, asyncio=asyncio, wraps=wraps,
                        logger=Mock(), pyrogram=pyrogram, enums=pyrogram.enums,
                        format_byte=str, update_upload_stat=AsyncMock())
        load('module/app.py', ['DownloadStatus','ForwardStatus','UploadStatus'], self.env)
        load('module/pyrogram_extension.py', ['UploadStopped','check_upload_active','confirmed_upload',
             'clean_confirmed_files','upload_telegram_chat','upload_telegram_chat_message',
             '_upload_telegram_chat_message','_upload_signal_message','download_thumbnail',
             'record_download_status','proc_cache_forward'], self.env)
        self.D,self.F,self.U = (self.env[k] for k in ('DownloadStatus','ForwardStatus','UploadStatus'))
        self.node = NS(chat_id=11, upload_telegram_chat_id=42, is_stop_transmission=False,
            reply_to_message=None, topic_id=0, has_protected_content=True, stat_forward=Mock(), bot=None,
            upload_user=None, delete_skipped_file_after_upload=False)
        self.app = NS(is_running=True, after_upload_telegram_delete=True, hide_file_name=False,
                      temp_save_path=self.temp.name, forward_limit_call=NS(wait=AsyncMock()))
        self.env['process_caption'] = AsyncMock(return_value='caption')
        self.path = Path(self.temp.name,'complete.bin'); self.path.write_bytes(b'good')
        self.message = NS(id=7, media=True, media_group_id=None, caption=None, caption_entities=[], text=None,
                          **{k:None for k in ('photo','video','animation','audio','document','voice','sticker','video_note')})
        self.message.audio = NS(file_id='audio',duration=1)
        self.receipt = NS(id=99, chat=NS(id=42), empty=False)
        self.client = NS(**{'send_'+kind:AsyncMock(return_value=self.receipt) for kind in
                           ('photo','video','animation','audio','document','voice','sticker','video_note')})

    async def test_failed_or_unconfirmed_upload_preserves_file(self):
        for response in (None, NS(id=99,chat=NS(id=99),empty=False)):
            self.client.send_audio.return_value=response
            await self.env['upload_telegram_chat'](self.client,self.client,self.app,self.node,
                self.message,self.D.SuccessDownload,str(self.path),True)
            self.assertTrue(self.path.exists())
            self.node.stat_forward.assert_called_with(self.F.FailedForward)
        self.client.send_audio.side_effect=OSError('fixture')
        await self.env['upload_telegram_chat'](self.client,self.client,self.app,self.node,
            self.message,self.D.SuccessDownload,str(self.path),True)
        self.assertTrue(self.path.exists())

    async def test_all_supported_single_media_return_receipt(self):
        for kind in ('photo','video','animation','audio','document','voice','sticker','video_note'):
            self.message.audio=None
            setattr(self.message,kind,NS(file_id='fixture',duration=1,width=1,height=1,thumbs=[]))
            result=await self.env['_upload_telegram_chat_message'](self.client,self.client,self.app,self.node,self.message,str(self.path))
            self.assertEqual(result,self.F.SuccessForward)
            method=getattr(self.client,'send_'+kind)
            method.assert_awaited_once()
            inspect.signature(getattr(pyrogram.Client, 'send_' + kind)).bind(
                None, *method.call_args.args, **method.call_args.kwargs)
            if kind in ('sticker','video_note'):
                self.assertNotIn('caption',method.call_args.kwargs)
            setattr(self.message,kind,None)
        with self.assertRaises(ValueError):
            await self.env['_upload_telegram_chat_message'](self.client,self.client,self.app,self.node,self.message,str(self.path))

    async def test_single_cleanup_requires_confirmed_receipt(self):
        await self.env['upload_telegram_chat'](self.client,self.client,self.app,self.node,
            self.message,self.D.SuccessDownload,str(self.path),True)
        self.assertFalse(self.path.exists())

    async def test_album_files_survive_cache_and_failed_send(self):
        self.node.media_group_ids_lock=asyncio.Lock()
        self.node.skip_msg_id=lambda mid:False
        self.node.pending_upload_files={7:str(self.path)}
        self.node.download_status={7:self.D.SuccessDownload,8:self.D.SuccessDownload}
        self.node.upload_status={7:self.U.SuccessUpload,8:self.U.Uploading}
        self.message.media_group_id='album'
        item=NS(message='caption',entities=None)
        self.node.media_group_ids={'album':{7:item,8:None}}
        sender=AsyncMock(return_value=None);self.env['send_media_group_v2']=sender
        self.assertEqual(await self.env['proc_cache_forward'](self.client,self.node,self.message,True,self.app),self.F.CacheForward)
        self.assertTrue(self.path.exists());sender.assert_not_awaited()
        self.node.upload_status[8]=self.U.FailedUpload
        await self.env['proc_cache_forward'](self.client,self.node,self.message,True,self.app)
        self.assertTrue(self.path.exists())
        self.node.media_group_ids={'album':{7:item}}
        sender.return_value=[self.receipt]
        await self.env['proc_cache_forward'](self.client,self.node,self.message,True,self.app)
        self.assertFalse(self.path.exists())

    async def test_cancelled_upload_queue_never_sends(self):
        load('media_downloader.py',['upload_worker'],self.env)
        self.node.is_stop_transmission=True
        queue=asyncio.Queue();await queue.put((self.client,self.message,self.node,self.D.SuccessDownload,str(self.path),4))
        self.env.update(app=self.app,upload_queue=queue,upload_queue_condition=asyncio.Condition(),report_bot_download_status=AsyncMock())
        task=asyncio.create_task(self.env['upload_worker']())
        try:
            await asyncio.wait_for(queue.join(),1)
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        self.client.send_audio.assert_not_awaited();self.assertTrue(self.path.exists())

    async def test_cancel_after_rate_wait_never_sends(self):
        async def stop(node):node.is_stop_transmission=True
        self.app.forward_limit_call.wait=stop
        result=await self.env['upload_telegram_chat_message'](self.client,self.client,self.app,self.node,self.message,str(self.path))
        self.assertEqual(result,self.F.StopForward)
        self.client.send_audio.assert_not_awaited()

    async def test_fetch_exception_records_failure_and_does_not_poison_cache(self):
        load('media_downloader.py',['download_media','download_task','worker'],self.env)
        cache=defaultdict(lambda:None);fetch=AsyncMock(side_effect=OSError('fixture'))
        queue=asyncio.Queue();uploads=asyncio.Queue()
        self.node.client=None;self.node.download_status={7:self.D.Downloading}
        self.app.media_types=['audio'];self.app.file_formats={};self.app.set_download_id=Mock()
        await queue.put((self.message,self.node))
        self.env.update(app=self.app,queue=queue,upload_queue=uploads,_download_cache=cache,
                        fetch_message=fetch,_wait_for_upload_queue_capacity=AsyncMock())
        self.env['download_media']=self.env['record_download_status'](self.env['download_media'])
        self.env['logger'].exception.side_effect=lambda *args:setattr(self.app,'is_running',False)
        await self.env['worker'](self.client)
        self.assertEqual(cache[(11,7)],self.D.FailedDownload)
        self.assertEqual(self.node.download_status[7],self.D.FailedDownload)
        self.assertEqual((await uploads.get())[3],self.D.FailedDownload)
        self.app.set_download_id.assert_called_once_with(self.node,7,self.D.FailedDownload)
        with self.assertRaises(OSError):
            await self.env['download_media'](self.client,self.message,['audio'],{},self.node)
        self.assertEqual(fetch.await_count,2)

    async def test_thumbnail_retry_uses_fresh_reference_and_removes_partial(self):
        old,fresh=NS(file_id='old',file_size=4),NS(file_id='fresh',file_size=4)
        self.env['fetch_message']=AsyncMock(side_effect=[NS(video=NS(thumbs=[old])),NS(video=NS(thumbs=[fresh]))])
        self.env['asyncio']=NS(sleep=AsyncMock())
        seen=[]
        async def download(media,*,file_name):
            seen.append(media.file_id);path=Path(file_name);path.parent.mkdir(exist_ok=True)
            self.assertFalse(path.exists())
            path.write_bytes(b'x' if media is old else b'good')
            return file_name
        result=await self.env['download_thumbnail'](NS(download_media=download),self.temp.name,NS(video=NS(thumbs=[old])))
        self.assertEqual(seen,['old','fresh']);self.assertEqual(Path(result).read_bytes(),b'good')

    async def test_failed_album_download_releases_ready_members(self):
        self.node.media_group_ids_lock=asyncio.Lock();self.node.skip_msg_id=lambda mid:False
        self.node.pending_upload_files={7:str(self.path)}
        self.node.download_status={7:self.D.SuccessDownload,8:self.D.FailedDownload}
        self.node.upload_status={7:self.U.SuccessUpload}
        self.node.media_group_ids={'album':{7:NS(message='',entities=None),8:None}}
        sender=AsyncMock(return_value=[self.receipt]);self.env['send_media_group_v2']=sender
        failed=NS(id=8,media=True,media_group_id='album')
        await self.env['upload_telegram_chat'](self.client,self.client,self.app,self.node,failed,self.D.FailedDownload)
        sender.assert_awaited_once();self.assertFalse(self.path.exists())

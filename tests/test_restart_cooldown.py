import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch
from tests.test_forwarding import load_executor


class RestartCooldownTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_rpc_is_marked_unsent_during_wait_and_in_flight_before_retry(self):
        from pyrogram import Client, errors, raw
        api=load_executor()
        client=Client('offline-resume',api_id=1,api_hash='fixture',in_memory=True)
        save=Mock()
        flood=api._FloodControl(200,None,save)
        progress=api._TransferProgress(None,flood=flood)
        await progress.stage('copy')
        scoped=api._unit_client(client,progress)
        rpc=raw.functions.messages.SendMessage(peer=raw.types.InputPeerSelf(),message='fixture',random_id=42)
        async def send(query, **kwargs):
            self.assertIs(query,rpc)
            self.assertEqual(save.call_args.kwargs,dict(safe_from=5,wait_until=0))
            if client.invoke.await_count==1:
                raise errors.FloodWait(90)
            return 'accepted'
        async def sleep(seconds):
            self.assertEqual(seconds,90)
            self.assertEqual(save.call_args.kwargs['safe_from'],3)
            self.assertGreater(save.call_args.kwargs['wait_until'],0)
        client.invoke=AsyncMock(side_effect=send)
        flood.begin_unit(3,5)
        with patch.object(api.asyncio,'sleep',AsyncMock(side_effect=sleep)):
            self.assertEqual(await scoped.invoke(rpc),'accepted')
        self.assertEqual(client.invoke.await_count,2)

    async def test_unconfirmed_indirect_rpc_does_not_rewind_the_safe_boundary(self):
        from pyrogram import errors
        api=load_executor()
        save=Mock()
        flood=api._FloodControl(100,None,save)
        progress=api._TransferProgress(None,flood=flood)
        flood.begin_unit(3,5)
        op=AsyncMock(side_effect=errors.FloodWait(90))
        with patch.object(api.asyncio,'sleep',AsyncMock(side_effect=asyncio.CancelledError)), \
             self.assertRaises(asyncio.CancelledError):
            await api._flood_retry(op,progress)
        self.assertIsNone(save.call_args.kwargs['safe_from'])
        self.assertGreater(save.call_args.kwargs['wait_until'],0)
        op.assert_awaited_once()

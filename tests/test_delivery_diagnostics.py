"""Receipt identities and safe read-failure classification."""
import unittest
from unittest.mock import AsyncMock
from tests import test_forwarding as fixtures

class DiagnosticTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):fixtures.ForwardingTests.setUp(self)

    async def run_request(self,ids):
        return await self.api.forward_messages('@source','@target',ids,client=self.client,group_mode='split')

    async def test_read_failure_is_safe_but_send_timeout_is_uncertain(self):
        self.client.get_messages.side_effect=OSError('password=private-secret')
        result=await self.run_request([1])
        error=result['failed'][0]
        self.assertEqual(error['stage'],'read');self.assertTrue(error['retryable'])
        self.assertTrue(error['frames']);self.assertEqual(len(error['diagnostic']),12)
        self.assertNotIn('private-secret',str(result))
        self.client.get_messages.side_effect=None
        item=fixtures.message(2);item.copy=AsyncMock(side_effect=TimeoutError('token=private-secret'))
        self.client.get_messages.return_value=[item]
        result=await self.run_request([2]);error=result['failed'][0]
        self.assertEqual(error['stage'],'send');self.assertFalse(error['retryable'])
        self.assertEqual(result['receipts'],[])
        item.copy.assert_awaited_once()
        self.assertNotIn('private-secret',str(result))

    async def test_receipts_link_original_ids_to_returned_destination_ids(self):
        result=await self.run_request([7,3])
        self.assertEqual(result['receipts'],[{'source':7,'chat':-1002,'id':10007},{'source':3,'chat':-1002,'id':10003}])
        self.assertEqual(result['sent'],2)

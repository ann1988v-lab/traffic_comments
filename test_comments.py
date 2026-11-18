import asyncio
from datetime import datetime, timezone
import io
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from cryptography.fernet import Fernet
from telethon import errors, functions, types, utils
from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from comments import Runner, group_posts, prepare
from delivery import input_peer, pack, report_csv
from main import Panel, PublicPanel, build_app
from storage import Store


def channel():
    return types.Channel(100, "Test channel", types.ChatPhotoEmpty(), None,
                         broadcast=True, username="sourcechannel", access_hash=111)


def group():
    return types.Channel(200, "Discussion", types.ChatPhotoEmpty(), None,
                         megagroup=True, access_hash=222)


def message(post_id, album=None, peer=100, post=True):
    return types.Message(post_id, types.PeerChannel(peer), datetime.now(timezone.utc),
                         "Publication", post=post, grouped_id=album)


def session_string():
    session = StringSession()
    session.set_dc(2, "127.0.0.1", 443)
    session.auth_key = AuthKey(bytes(range(256)))
    return session.save()


class Client:
    def __init__(self, joined=False, require_join=False):
        self.posts = [message(1), message(2), message(3)]
        self.joined = joined
        self.require_join = require_join
        self.closed = set()
        self.fail = {}
        self.calls = []
        self.sent = []
        self.connect = AsyncMock()
        self.disconnect = AsyncMock()
        self.is_user_authorized = AsyncMock(return_value=True)

    async def get_entity(self, value):
        return channel()

    async def iter_dialogs(self):
        yield SimpleNamespace(entity=channel())
        if self.joined:
            yield SimpleNamespace(entity=group())

    async def get_messages(self, peer, limit):
        return sorted(self.posts, key=lambda m: m.id, reverse=True)[:limit]

    async def iter_messages(self, peer, min_id, reverse, limit):
        for post in sorted((m for m in self.posts if m.id > min_id), key=lambda m: m.id)[:limit]:
            yield post

    async def __call__(self, request):
        self.calls.append(request)
        if isinstance(request, functions.messages.GetDiscussionMessageRequest):
            if request.msg_id in self.closed:
                raise errors.MsgIdInvalidError(None)
            if self.require_join and not self.joined:
                raise errors.UserNotParticipantError(None)
            return SimpleNamespace(messages=[message(request.msg_id),
                message(10000 + request.msg_id, peer=200, post=False)], chats=[channel(), group()])
        if isinstance(request, functions.channels.GetFullChannelRequest):
            return SimpleNamespace(full_chat=SimpleNamespace(linked_chat_id=200), chats=[channel(), group()])
        if isinstance(request, functions.channels.JoinChannelRequest):
            self.joined = True
            return SimpleNamespace(chats=[group()])
        raise AssertionError(type(request).__name__)

    async def send_message(self, peer, text, **kwargs):
        post_id = kwargs["reply_to"] - 10000
        if post_id in self.fail:
            raise self.fail[post_id]
        self.sent.append({"peer": peer, "text": text, **kwargs})

    async def send_file(self, peer, file, **kwargs):
        self.sent.append({"peer": peer, "data": file.read(), **kwargs})


class CommentsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.key = Fernet.generate_key()
        self.store = Store(self.temp.name, self.key)
        self.addCleanup(self.store.close)
        self.store.save_account(10, "Account", {"session": session_string(),
            "options": {"api_id": 1, "api_hash": "a" * 32}})

    async def create_job(self, client, job="test", mode="latest", count=3, media=None):
        issues = []
        channels = await prepare(client, ["@sourcechannel"], issues, mode, count)
        snapshot = {"mode": mode, "post_count": count, "message": "Comment", "delay": 0,
                    "poll_seconds": 0, "media": media,
                    "accounts": [{"account_id": 10, "items": channels, "issues": issues}]}
        self.store.new_job(job, snapshot)
        return snapshot

    async def run_job(self, client, job="test"):
        runner = Runner(self.store, AsyncMock(), AsyncMock())
        with patch("comments.make_client", return_value=client):
            runner.launch(job)
            await runner.task
        return runner

    async def test_latest_plan_is_read_only_and_chooses_actual_latest_post(self):
        client = Client()
        snapshot = await self.create_job(client)
        self.assertEqual([p["post_id"] for p in snapshot["accounts"][0]["items"][0]["posts"]], [3])
        self.assertFalse(client.joined)
        self.assertEqual(client.sent, [])
        self.assertFalse(any(isinstance(r, functions.channels.JoinChannelRequest) for r in client.calls))

    async def test_join_then_reply_to_correct_thread_never_plain_group_message(self):
        client = Client()
        await self.create_job(client)
        await self.run_job(client)
        self.assertTrue(client.joined)
        self.assertEqual(len(client.sent), 1)
        self.assertEqual(client.sent[0]["peer"].channel_id, 200)
        self.assertEqual(client.sent[0]["reply_to"], 10003)
        self.assertEqual(self.store.job("test")["state"], "done")

    async def test_membership_required_before_discussion_lookup_is_supported(self):
        client = Client(require_join=True)
        snapshot = await self.create_job(client)
        self.assertIsNone(snapshot["accounts"][0]["items"][0]["posts"][0]["reply_id"])
        self.assertFalse(client.joined)
        await self.run_job(client)
        self.assertEqual(client.sent[0]["reply_to"], 10003)
        joins = [r for r in client.calls if isinstance(r, functions.channels.JoinChannelRequest)]
        self.assertEqual(len(joins), 1)

    async def test_recent_posts_use_same_group_after_single_join(self):
        client = Client()
        await self.create_job(client, mode="recent", count=3)
        await self.run_job(client)
        self.assertEqual([s["reply_to"] for s in client.sent], [10001, 10002, 10003])
        self.assertEqual(len([r for r in client.calls if isinstance(r, functions.channels.JoinChannelRequest)]), 1)

    async def test_closed_comments_are_reported_and_other_posts_continue(self):
        client = Client(joined=True)
        client.closed.add(3)
        await self.create_job(client, mode="recent", count=3)
        await self.run_job(client)
        self.assertEqual([s["reply_to"] for s in client.sent], [10001, 10002])
        rows = self.store.deliveries("test")
        self.assertEqual(sum(r["status"] == "failed" for r in rows), 1)
        self.assertIn("https://t.me/sourcechannel/3", report_csv(self.store, "test").decode("utf-8-sig"))

    async def test_one_send_error_does_not_stop_other_posts(self):
        client = Client(joined=True)
        client.fail[2] = errors.ChatWriteForbiddenError(None)
        await self.create_job(client, mode="recent", count=3)
        await self.run_job(client)
        self.assertEqual([s["reply_to"] for s in client.sent], [10001, 10003])
        self.assertEqual(self.store.job("test")["state"], "done")

    async def test_new_job_cannot_duplicate_a_successful_comment(self):
        client = Client(joined=True)
        await self.create_job(client)
        await self.run_job(client)
        await self.create_job(client, job="second")
        await self.run_job(client, "second")
        self.assertEqual(len(client.sent), 1)
        self.assertEqual(self.store.deliveries("second")[0]["status"], "skipped")

    async def test_uncertain_send_is_not_repeated_in_new_job(self):
        client = Client(joined=True)
        client.fail[3] = TimeoutError()
        await self.create_job(client)
        await self.run_job(client)
        self.assertEqual(self.store.deliveries("test")[0]["status"], "pending")
        client.fail.clear()
        await self.create_job(client, job="second")
        await self.run_job(client, "second")
        self.assertEqual(client.sent, [])
        self.assertEqual(self.store.deliveries("second")[0]["status"], "skipped")

    async def test_photo_comment_preserves_caption_and_thread(self):
        client = Client(joined=True)
        media_id = self.store.save_media(b"photo data")
        await self.create_job(client, media={"id": media_id, "kind": "photo", "name": "photo.jpg"})
        await self.run_job(client)
        self.assertEqual(client.sent[0]["data"], b"photo data")
        self.assertEqual(client.sent[0]["caption"], "Comment")
        self.assertEqual(client.sent[0]["reply_to"], 10003)

    async def test_monitor_baseline_only_new_posts_and_resume_after_downtime(self):
        client = Client(joined=True)
        client.posts = [message(1)]
        await self.create_job(client, mode="watch")
        runner = Runner(self.store, AsyncMock())
        waits = 0
        async def wait(stop, seconds):
            nonlocal waits
            waits += 1
            if waits == 1:
                client.posts.append(message(2))
                return
            stop.set()
            raise asyncio.CancelledError()
        with patch("comments.make_client", return_value=client), patch("comments.wait_or_stop", wait):
            runner.launch("test")
            await runner.task
        self.assertEqual([s["reply_to"] for s in client.sent], [10002])
        self.assertEqual(self.store.cursor("test", 10, 100), 2)
        client.posts.append(message(3))
        async def stop_immediately(stop, seconds):
            stop.set()
            raise asyncio.CancelledError()
        runner = Runner(self.store, AsyncMock())
        with patch("comments.make_client", return_value=client), patch("comments.wait_or_stop", stop_immediately):
            runner.launch("test")
            await runner.task
        self.assertEqual([s["reply_to"] for s in client.sent], [10002, 10003])

    async def test_album_is_one_post_and_not_duplicated_across_polling_batches(self):
        client = Client(joined=True)
        client.posts = [message(1, album=999), message(2, album=999), message(3)]
        self.assertEqual(len(group_posts(client.posts)), 2)
        channel_info = pack(channel(), True, "@sourcechannel", "public", "sourcechannel")
        self.store.new_job("monitor", {"mode": "watch"})
        self.store.set_cursor("monitor", 10, 100, 0)
        runner = Runner(self.store, AsyncMock())
        runner.job_id = "monitor"
        dialogs = {utils.get_peer_id(group()): group()}
        snapshot = {"message": "Comment", "delay": 0}
        client.posts = [message(1, album=999)]
        await runner.watch_channel(client, "monitor", 10, channel_info, snapshot, dialogs)
        client.posts.append(message(2, album=999))
        await runner.watch_channel(client, "monitor", 10, channel_info, snapshot, dialogs)
        self.assertEqual(len(client.sent), 1)

    async def test_switch_modes_through_callback_and_preview_entry_points(self):
        client = Client(joined=True)
        panel = Panel(self.store, 10, 1, "a" * 32)
        settings = self.store.settings()
        settings.update(message="Comment", targets=["@sourcechannel"], post_count=2)
        self.store.set_settings(settings)
        for mode, expected_count in (("latest", 1), ("recent", 2), ("watch", 0)):
            output = SimpleNamespace(reply_text=AsyncMock(), reply_document=AsyncMock())
            update = SimpleNamespace(effective_user=SimpleNamespace(id=10),
                effective_chat=SimpleNamespace(type="private"), callback_query=SimpleNamespace(
                    data="mode:" + mode, message=output, answer=AsyncMock()))
            await panel.callback(update, None)
            self.assertEqual(self.store.settings()["mode"], mode)
            with patch("main.make_client", return_value=client):
                await panel.preview(output)
            snapshot = self.store.job(self.store.jobs()[0]["id"])["snapshot"]
            self.assertEqual(snapshot["mode"], mode)
            self.assertEqual(len(snapshot["accounts"][0]["items"][0]["posts"]), expected_count)

    async def test_public_users_have_separate_accounts_and_comment_history(self):
        public = PublicPanel(Path(self.temp.name) / "public", self.key, 1, "a" * 32)
        app = SimpleNamespace(bot=SimpleNamespace(send_message=AsyncMock(), send_document=AsyncMock()))
        try:
            first = await public.user_panel(10, app)
            second = await public.user_panel(20, app)
            first.store.save_account(123, "First", {"session": "private"})
            first.store.claim_comment(123, 100, "post:3", "job")
            self.assertEqual(second.store.accounts(), [])
            self.assertTrue(second.store.claim_comment(123, 100, "post:3", "other-job"))
        finally:
            await public.post_stop(app)
            await public.post_shutdown(app)

    async def test_application_builds_with_no_owner_variable(self):
        with patch.dict(os.environ, {"BOT_TOKEN": "123456789:" + "x" * 35, "OWNER_ID": "",
                        "ENCRYPTION_KEY": self.key.decode(), "TELEGRAM_API_ID": "1",
                        "TELEGRAM_API_HASH": "a" * 32, "DATA_DIR": str(Path(self.temp.name) / "app")}):
            app = build_app()
            self.assertGreater(len(app.handlers[0]), 5)
            await app.post_shutdown(app)

    async def test_empty_monitor_plan_is_rejected_before_background_task(self):
        class NoComments(Client):
            async def __call__(self, request):
                if isinstance(request, functions.channels.GetFullChannelRequest):
                    return SimpleNamespace(full_chat=SimpleNamespace(linked_chat_id=None), chats=[channel()])
                return await super().__call__(request)
        panel = Panel(self.store, 10, 1, "a" * 32)
        self.store.set_settings({"message": "Comment", "targets": ["@sourcechannel"], "mode": "watch"})
        output = SimpleNamespace(reply_text=AsyncMock(), reply_document=AsyncMock())
        with patch("main.make_client", return_value=NoComments()):
            await panel.preview(output)
        self.assertEqual(self.store.jobs(), [])
        self.assertIn("Нет доступных каналов", output.reply_text.call_args.args[0])


if __name__ == "__main__":
    unittest.main()

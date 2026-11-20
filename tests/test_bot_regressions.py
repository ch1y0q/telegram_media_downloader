"""Offline bot regressions: no real Telegram, configuration or session access."""

import ast
import inspect
import json
import re
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

ROOT = Path(__file__).resolve().parents[1]


class StopPropagation(Exception):
    """Model Pyrogram's explicit cross-group propagation stop."""


class FakeFilter:
    def __init__(self, predicate):
        self.predicate = predicate

    async def __call__(self, client, message):
        result = self.predicate(client, message)
        return await result if inspect.isawaitable(result) else result

    def __and__(self, other):
        async def both(client, message):
            return await self(client, message) and await other(client, message)

        return FakeFilter(both)

    def __invert__(self):
        async def inverse(client, message):
            return not await self(client, message)

        return FakeFilter(inverse)


class FakeFilters:
    text = FakeFilter(lambda client, message: bool(message.text))
    media = FakeFilter(lambda client, message: bool(message.media))
    private = FakeFilter(lambda client, message: message.chat.type == "private")

    @staticmethod
    def create(callback):
        return FakeFilter(lambda client, message: callback(None, client, message))

    @staticmethod
    def regex(pattern):
        return FakeFilter(
            lambda client, message: bool(re.search(pattern, message.text or ""))
        )

    @staticmethod
    def user(allowed):
        return FakeFilter(
            lambda client, message: bool(
                message.from_user and message.from_user.id in allowed
            )
        )

    @staticmethod
    def command(commands):
        def matches(client, message):
            text = message.text or ""
            token = text.split(maxsplit=1)[0] if text else ""
            command, _, target = token.partition("@")
            return (
                command.startswith("/")
                and command[1:] in commands
                and (not target or target.lower() == "test_bot")
            )

        return FakeFilter(matches)


class FakeHandler:
    def __init__(self, callback, filters=None):
        self.callback = callback
        self.filters = filters


class FakeCallbackQueryHandler(FakeHandler):
    pass


class FakeMarkup:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class FakeClient:
    """Only recorded operations are possible; no Pyrogram code is imported."""

    def __init__(self, *args, **kwargs):
        self.handlers = []
        self.send_message = AsyncMock(
            return_value=types.SimpleNamespace(id=7, text="sent")
        )
        self.get_chat = AsyncMock(
            return_value=types.SimpleNamespace(
                id=-1000000000123,
                title="Fixture chat",
                username="fixture",
                type="channel",
                has_protected_content=False,
            )
        )
        self.get_messages = AsyncMock()
        self.get_me = AsyncMock(return_value=types.SimpleNamespace(id=11))
        self.start = AsyncMock()
        self.set_bot_commands = AsyncMock()

    def add_handler(self, handler, group=0):
        self.handlers.append((group, handler))

    async def dispatch(self, message):
        """Pyrogram invokes only the first matching handler within each group."""
        for group in sorted({group for group, _ in self.handlers}):
            for current_group, handler in self.handlers:
                if current_group != group or isinstance(
                    handler, FakeCallbackQueryHandler
                ):
                    continue
                if await handler.filters(self, message):
                    try:
                        await handler.callback(self, message)
                    except StopPropagation:
                        return
                    break


class FakeYaml:
    """JSON-backed fixture writer: never attempts to read runtime YAML."""

    @staticmethod
    def dump(data, stream):
        json.dump(data, stream)


def fake_module(name, **attributes):
    module = types.ModuleType(name)
    module.__dict__.update(attributes)
    return module


def load_bot_safely():
    """Execute the complete real modules with all app dependencies substituted."""
    language_path = ROOT / "module/language.py"
    language = fake_module("_offline_downloader_language")
    exec(
        compile(language_path.read_text(encoding="utf-8"), str(language_path), "exec"),
        language.__dict__,
    )
    task_type = Enum("TaskType", "Download Forward ListenForward")
    pyrogram_types = fake_module(
        "pyrogram.types",
        Message=object,
        CallbackQuery=object,
        InlineKeyboardButton=FakeMarkup,
        InlineKeyboardMarkup=FakeMarkup,
        KeyboardButton=FakeMarkup,
        ReplyKeyboardMarkup=FakeMarkup,
        BotCommand=FakeMarkup,
    )
    pyrogram = fake_module(
        "pyrogram",
        Client=FakeClient,
        types=pyrogram_types,
        filters=FakeFilters,
        enums=types.SimpleNamespace(
            ParseMode=types.SimpleNamespace(HTML="html"),
            ChatType=types.SimpleNamespace(BOT="bot", CHANNEL="channel"),
        ),
    )
    app = fake_module(
        "module.app",
        Application=object,
        ChatDownloadConfig=types.SimpleNamespace,
        ForwardStatus=Enum("ForwardStatus", "FailedForward SkipForward"),
        QueryHandler=task_type,
        QueryHandlerStr=Mock(),
        TaskNode=Mock(),
        TaskType=task_type,
        UploadStatus=Enum("UploadStatus", "SkipUpload"),
    )
    extension_names = (
        "check_user_permission get_utf16_length parse_link proc_cache_forward "
        "report_bot_forward_status report_bot_status retry set_meta_data "
        "upload_telegram_chat_message"
    ).split()
    extension = fake_module(
        "module.pyrogram_extension", **{name: AsyncMock() for name in extension_names}
    )
    yaml = fake_module("ruamel.yaml", YAML=FakeYaml)
    modules = {
        "module": fake_module("module", __path__=[]),
        "module.app": app,
        "module.filter": fake_module("module.filter", Filter=Mock),
        "module.get_chat_history_v2": fake_module(
            "module.get_chat_history_v2", get_chat_history_v2=AsyncMock()
        ),
        "module.language": language,
        "module.pyrogram_extension": extension,
        "utils": fake_module("utils", __path__=[], __version__="offline-test"),
        "utils.format": fake_module(
            "utils.format",
            replace_date_time=lambda value: value,
            validate_title=lambda value: value,
        ),
        "utils.meta_data": fake_module("utils.meta_data", MetaData=Mock),
        "pyrogram": pyrogram,
        "pyrogram.types": pyrogram_types,
        "pyrogram.handlers": fake_module(
            "pyrogram.handlers",
            CallbackQueryHandler=FakeCallbackQueryHandler,
            MessageHandler=FakeHandler,
        ),
        "loguru": fake_module("loguru", logger=Mock()),
        "ruamel": fake_module("ruamel", yaml=yaml),
        "ruamel.yaml": yaml,
    }
    bot_path = ROOT / "module/bot.py"
    source = bot_path.read_text(encoding="utf-8")
    bot = fake_module("_offline_downloader_bot")
    with patch.dict(sys.modules, modules), patch(
        "builtins.open", side_effect=AssertionError("Runtime file access during import")
    ):
        exec(compile(source, str(bot_path), "exec"), bot.__dict__)
    return bot, language


def message(text="", uid=11, chat_type="private", **attributes):
    def stop_propagation():
        raise StopPropagation()

    defaults = {
        "text": text,
        "id": 42,
        "media": None,
        "chat": types.SimpleNamespace(id=uid, type=chat_type),
        "from_user": types.SimpleNamespace(id=uid),
        "stop_propagation": stop_propagation,
    }
    defaults.update(attributes)
    return types.SimpleNamespace(**defaults)


class BotRegressions(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.module, self.language = load_bot_safely()
        self.bot = self.module._bot
        self.bot.update_config = Mock()
        self.bot.client = FakeClient()
        self.client = FakeClient()

    async def register_handlers(self):
        def close_background_coroutine(coroutine):
            coroutine.close()
            return Mock()

        app = types.SimpleNamespace(
            application_name="offline",
            api_hash="fixture",
            api_id=1,
            bot_token="fixture",
            session_file_path="never-opened",
            proxy=None,
            allowed_user_ids=[],
            loop=types.SimpleNamespace(
                create_task=Mock(side_effect=close_background_coroutine)
            ),
        )
        with patch.object(self.module.os.path, "exists", return_value=False), patch(
            "builtins.open",
            side_effect=AssertionError("Runtime file access during startup"),
        ):
            await self.bot.start(
                app, self.bot.client, AsyncMock(), AsyncMock(), AsyncMock()
            )
        self.bot.bot.send_message.reset_mock()
        return self.bot.bot

    def test_public_forward_has_one_public_link(self):
        result = self.module.format_forward_ids(
            message(
                forward_from_chat=types.SimpleNamespace(
                    id=-1000000000123,
                    title="Public",
                    username="public_chat",
                    broadcast=True,
                ),
                forward_from_message_id=77,
                forward_date=datetime(2025, 1, 2, tzinfo=timezone.utc),
            )
        )
        self.assertIn("https://t.me/public_chat/77", result)
        self.assertEqual(result.count("https://"), 1)
        self.assertIn("chat_id: -1000000000123", result)
        self.assertIn("原消息时间:", result)

    def test_private_forward_has_internal_link(self):
        result = self.module.format_forward_ids(
            message(
                forward_from_chat=types.SimpleNamespace(
                    id=-1000000000123, title="Private", username=None, type="channel"
                ),
                forward_from_message_id=77,
            )
        )
        self.assertIn("https://t.me/c/123/77", result)
        self.assertEqual(result.count("https://"), 1)
        self.assertIn("来源频道:", result)

    def test_basic_group_and_missing_message_ids_have_no_bogus_link(self):
        for chat_id, username, mid in (
            (-123, None, 77),
            (-1000000000123, None, None),
            (-1000000000123, "public_chat", None),
            (-1000000000123, None, 0),
            (-1000000000123, "public_chat", -1),
        ):
            with self.subTest(chat_id=chat_id, username=username, mid=mid):
                result = self.module.format_forward_ids(
                    message(
                        forward_from_chat=types.SimpleNamespace(
                            id=chat_id, title="Fixture", username=username
                        ),
                        forward_from_message_id=mid,
                    )
                )
                self.assertNotIn("https://", result)
                self.assertIn("chat_id: " + str(chat_id), result)

    def test_hidden_user_and_nonforward_sources(self):
        hidden = self.module.format_forward_ids(message(forward_sender_name="Hidden"))
        self.assertIn("来源被隐藏", hidden)
        self.assertNotIn("https://", hidden)
        user = self.module.format_forward_ids(
            message(
                forward_from=types.SimpleNamespace(
                    id=17, first_name="Test", last_name="User", username="fixture_user"
                )
            )
        )
        self.assertIn("user_id: 17", user)
        self.assertIn("username: @fixture_user", user)
        self.assertNotIn("https://", user)
        self.assertIn("无法解析", self.module.format_forward_ids(message()))

    def test_destination_normalization(self):
        cases = {
            " -1000000000123 ": -1000000000123,
            "-123": -123,
            "123": 123,
            "@fixture_chat": "@fixture_chat",
            "fixture_chat": "@fixture_chat",
            "https://t.me/fixture_chat": "@fixture_chat",
            "http://t.me/fixture_chat/42?single": "@fixture_chat",
            "t.me/fixture_chat/": "@fixture_chat",
            "https://t.me/s/fixture_chat/42": "@fixture_chat",
            "https://t.me/c/123/42": -1000000000123,
            "https://t.me/c/123/42/43": -1000000000123,
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(self.module.normalize_destination(value), expected)
        for value in (
            "",
            "0",
            "https://evil.example/fixture_chat",
            "https://t.me.evil/fixture",
            "https://t.me@evil/fixture",
            "https://t.me/",
            "https://t.me/s",
            "https://t.me/c/nope/42",
            "https://t.me/c/0/42",
            "https://t.me/fixture_chat/nan",
            "https://t.me/+invite",
            "two names",
            "ftp://t.me/fixture_chat",
            "https://t.me/fixture//42",
        ):
            with self.subTest(invalid=value):
                with self.assertRaises(ValueError):
                    self.module.normalize_destination(value)

    async def test_pending_link_sets_destination_before_download(self):
        client = await self.register_handlers()
        ctx = self.bot.get_user_ctx(11)
        ctx["await_dst"] = True
        self.module.parse_link.reset_mock()
        await client.dispatch(message("https://t.me/fixture_chat/42"))
        self.bot.client.get_chat.assert_awaited_once_with("@fixture_chat")
        self.assertFalse(ctx["await_dst"])
        self.assertEqual(ctx["dst"]["id"], -1000000000123)
        self.module.parse_link.assert_not_awaited()
        self.assertEqual(client.send_message.await_count, 1)
        self.assertLess(
            next(
                group
                for group, handler in client.handlers
                if handler.callback is self.module.capture_destination
            ),
            next(
                group
                for group, handler in client.handlers
                if handler.callback is self.module.download_from_link
            ),
        )

    async def test_normal_download_link_is_not_swallowed_by_destination_capture(self):
        client = await self.register_handlers()
        self.bot.get_user_ctx(11)["await_dst"] = False
        self.module.parse_link.return_value = (None, None, None)
        await client.dispatch(message("https://t.me/fixture_chat/42"))
        self.module.parse_link.assert_awaited_once()
        self.bot.client.get_chat.assert_not_awaited()

    async def test_pending_capture_is_per_user_private_and_authorized(self):
        client = await self.register_handlers()
        self.bot.user_context["99"] = {
            "mode": "download",
            "dst": None,
            "await_dst": True,
        }
        for msg in (
            message("https://t.me/fixture_chat/42", uid=99),
            message("https://t.me/fixture_chat/42", chat_type="group"),
        ):
            handler = next(
                handler
                for _, handler in client.handlers
                if handler.callback is self.module.capture_destination
            )
            self.assertFalse(await handler.filters(client, msg))
        await client.dispatch(message("https://t.me/fixture_chat/42", uid=99))
        self.module.parse_link.assert_not_awaited()
        self.bot.client.get_chat.assert_not_awaited()
        self.assertTrue(self.bot.user_context["99"]["await_dst"])

    async def test_pending_invalid_destination_stays_pending_without_download(self):
        client = await self.register_handlers()
        ctx = self.bot.get_user_ctx(11)
        ctx["await_dst"] = True
        old_destination = {"type": "disk", "title": "old"}
        ctx["dst"] = old_destination
        self.module.parse_link.reset_mock()
        await client.dispatch(message("https://t.me.evil/fixture_chat"))
        self.assertTrue(ctx["await_dst"])
        self.assertIs(ctx["dst"], old_destination)
        self.bot.client.get_chat.assert_not_awaited()
        self.module.parse_link.assert_not_awaited()
        self.assertIn(
            "Destination verify failed", client.send_message.await_args.args[1]
        )

    async def test_cancel_commands_and_chinese_preserve_same_user_context(self):
        client = await self.register_handlers()
        ctx = self.bot.get_user_ctx(11)
        old_destination = {"type": "disk", "title": "server disk"}
        ctx.update(mode="id", dst=old_destination)
        other = {"mode": "download", "dst": None, "await_dst": True}
        self.bot.user_context["22"] = other
        for text in ("/cancel", "/cancel@test_bot", "取消"):
            with self.subTest(text=text):
                ctx["await_dst"] = True
                self.bot.update_config.reset_mock()
                await client.dispatch(message(text))
                self.assertIs(self.bot.get_user_ctx(11), ctx)
                self.assertIs(ctx["dst"], old_destination)
                self.assertEqual(ctx["mode"], "id")
                self.assertFalse(ctx["await_dst"])
                self.assertTrue(other["await_dst"])
                self.bot.update_config.assert_called_once_with()
        self.bot.client.get_chat.assert_not_awaited()

    async def test_help_keyboard_sends_exactly_once(self):
        await self.module.forward_ids_text(self.client, message("📖 帮助"))
        self.client.send_message.assert_awaited_once()
        self.assertIsInstance(self.client.send_message.await_args.args[1], str)
        self.assertIn("/help", self.client.send_message.await_args.args[1])

    async def test_start_has_single_registered_menu_handler(self):
        client = await self.register_handlers()
        matching = []
        for _, handler in client.handlers:
            if not isinstance(
                handler, FakeCallbackQueryHandler
            ) and await handler.filters(client, message("/start@test_bot")):
                matching.append(handler.callback)
        self.assertEqual(matching, [self.module.ux_menu_command])
        await client.dispatch(message("/start"))
        client.send_message.assert_awaited_once()
        self.assertIn("Control panel", client.send_message.await_args.args[1])

    async def test_disk_receipt_and_panel_use_bare_translated_label(self):
        for language in (self.language.Language.EN, self.language.Language.ZH):
            with self.subTest(language=language):
                self.language.set_language(language)
                ctx = self.bot.get_user_ctx(11)
                ctx["await_dst"] = True
                self.client.send_message.reset_mock()
                await self.module.forward_ids_text(self.client, message("【本机】"))
                label = self.language._t("server disk")
                self.assertEqual(ctx["dst"]["title"], label)
                text = self.client.send_message.await_args.args[1]
                self.assertEqual(
                    text.splitlines()[0],
                    self.language._t("Destination locked") + ": " + label,
                )
                self.assertNotIn("Destination: Destination:", text)
                self.assertNotIn("目的地: 目的地:", text)
        ctx["dst"]["title"] = "Destination: stale language"
        self.assertNotIn("stale language", self.module.ux_status_text(11))

    async def test_forwarded_link_in_id_mode_reports_source_instead_of_download(self):
        client = await self.register_handlers()
        self.bot.get_user_ctx(11)["mode"] = "id"
        await client.dispatch(
            message(
                "https://t.me/fixture_chat/42",
                forward_sender_name="Hidden",
                forward_date=datetime.now(timezone.utc),
            )
        )
        self.module.parse_link.assert_not_awaited()
        self.assertIn("来源被隐藏", client.send_message.await_args.args[1])

    async def test_download_invalid_argument_count_returns_before_parsing(self):
        await self.module.download_from_link(
            self.client, message("https://t.me/fixture_chat/42 extra")
        )
        self.client.send_message.assert_awaited_once()
        self.module.parse_link.assert_not_awaited()

    async def test_download_missing_message_awaits_error_response(self):
        self.module.parse_link.return_value = (-1000000000123, 42, None)
        self.module.retry.return_value = None
        await self.module.download_from_link(
            self.client, message("https://t.me/fixture/42")
        )
        self.client.send_message.assert_awaited_once()
        self.assertIn("error", self.client.send_message.await_args.args[1])

    async def test_download_host_lookalike_does_not_match(self):
        client = await self.register_handlers()
        self.bot.get_user_ctx(11)
        await client.dispatch(message("https://t.me.evil/fixture_chat/42"))
        self.module.parse_link.assert_not_awaited()
        client.send_message.assert_not_awaited()

    async def test_invalid_forward_filter_never_creates_task(self):
        self.module.parse_link.return_value = (-1000000000123, None, None)
        self.bot.filter.check_filter.return_value = (False, "invalid fixture filter")
        result = await self.module.get_forward_task_node(
            self.client,
            message("/forward"),
            self.module.TaskType.Forward,
            "https://t.me/source",
            "https://t.me/target",
            download_filter="bad filter",
        )
        self.assertIsNone(result)
        self.module.TaskNode.assert_not_called()
        self.assertEqual(self.bot.task_node, {})
        self.client.send_message.assert_awaited_once()

    async def test_remove_missing_ad_filter_reports_without_adding_it(self):
        self.bot.app = types.SimpleNamespace(
            replace_advertisement_list=["existing"], update_config=Mock()
        )
        self.module.proc_replace_advertisement = AsyncMock(return_value="absent")
        await self.module.remove_replace_advertisement_filter(
            self.client, message("/remove_replace_ad https://t.me/source/42 absent")
        )
        self.assertEqual(self.bot.app.replace_advertisement_list, ["existing"])
        self.client.send_message.assert_awaited_once_with(
            11, "Filter : absent not exist"
        )

    async def test_stop_task_callbacks_edit_the_conversation_not_bot_user(self):
        self.client.edit_message_text = AsyncMock()
        node = types.SimpleNamespace(
            is_finish=lambda: False,
            task_type=self.module.TaskType.Download,
            stop_transmission=Mock(),
        )
        for data, has_task in (
            ("stop_download", True),
            ("stop_download", False),
            ("stop_download task 1", True),
        ):
            with self.subTest(data=data, has_task=has_task):
                self.bot.task_node = {1: node} if has_task else {}
                self.client.edit_message_text.reset_mock()
                query = types.SimpleNamespace(
                    data=data,
                    message=message(uid=11, from_user=types.SimpleNamespace(id=9000)),
                )
                await self.module.stop_task(
                    self.client, query, "stop_download", self.module.TaskType.Download
                )
                self.assertEqual(
                    self.client.edit_message_text.await_args.args[:2], (11, 42)
                )
        node.stop_transmission.assert_called_once_with()

    def test_context_persistence_uses_config_path_and_atomic_replace(self):
        with tempfile.TemporaryDirectory(
            prefix="downloader-bot-regression-"
        ) as directory:
            path = Path(directory) / "fixture.json"
            self.bot.config_path = str(path)
            ctx = {"mode": "id", "dst": {"type": "disk"}, "await_dst": True}
            self.bot.user_context = {"11": ctx}
            self.bot.config = {"unrelated": "preserved"}
            self.module.DownloadBot.update_config(self.bot)
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["unrelated"], "preserved")
            self.assertEqual(saved["user_context"]["11"], ctx)
            self.assertIs(self.bot.config["user_context"]["11"], ctx)
            self.assertEqual(
                {entry.name for entry in Path(directory).iterdir()}, {"fixture.json"}
            )

    async def test_real_ruamel_cancellation_preserves_comments_and_context(self):
        try:
            from ruamel.yaml import YAML
        except ImportError:
            self.skipTest("Optional ruamel.yaml dependency is not installed")
        self.bot._yaml = YAML()
        self.bot.config = self.bot._yaml.load(
            "# fixture comment\nuser_context:\n  '11':\n    mode: id\n"
            "    dst: {type: disk}  # destination comment\n    await_dst: true\n"
        )
        self.bot.assign_config(self.bot.config)
        ctx = self.bot.get_user_ctx(11)
        with tempfile.TemporaryDirectory(
            prefix="downloader-yaml-regression-"
        ) as directory:
            path = Path(directory) / "fixture.yaml"
            self.bot.config_path = str(path)
            self.bot.update_config = types.MethodType(
                self.module.DownloadBot.update_config, self.bot
            )
            await self.module.cancel_destination(self.client, message("/cancel"))
            self.assertIs(self.bot.get_user_ctx(11), ctx)
            self.assertFalse(ctx["await_dst"])
            saved = path.read_text(encoding="utf-8")
            self.assertIn("# fixture comment", saved)
            self.assertIn("# destination comment", saved)
            self.assertFalse(
                self.bot._yaml.load(saved)["user_context"]["11"]["await_dst"]
            )
            self.assertEqual(
                {entry.name for entry in Path(directory).iterdir()}, {"fixture.yaml"}
            )

    def test_translation_merge_and_nonempty_fallback(self):
        for language, expected in (
            (self.language.Language.RU, "Выбран язык"),
            (self.language.Language.UA, "Обрано мову"),
            (self.language.Language.ZH, "语言已设置为"),
        ):
            with self.subTest(language=language):
                self.language.set_language(language)
                self.assertEqual(self.language._t("Language set to"), expected)
                for text in self.language.translations:
                    self.assertTrue(self.language._t(text), text)
                self.assertEqual(self.language._t("unknown fixture"), "unknown fixture")
        self.language.set_language(self.language.Language.RU)
        self.assertEqual(self.language._t("server disk"), "server disk")
        tree = ast.parse((ROOT / "module/language.py").read_text(encoding="utf-8"))
        translation_dict = next(
            node.value
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "translations"
                for target in node.targets
            )
        )
        keys = [key.value for key in translation_dict.keys]
        self.assertEqual(len(keys), len(set(keys)), "Duplicate translation keys")

    def test_all_message_sends_are_awaited(self):
        tree = ast.parse((ROOT / "module/bot.py").read_text(encoding="utf-8"))
        parents = {
            child: node
            for node in ast.walk(tree)
            for child in ast.iter_child_nodes(node)
        }
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("send_message", "edit_message_text")
            ):
                self.assertIsInstance(
                    parents[node], ast.Await, "line " + str(node.lineno)
                )
                if node.func.attr == "send_message":
                    self.assertGreaterEqual(len(node.args) + len(node.keywords), 2)


if __name__ == "__main__":
    unittest.main()

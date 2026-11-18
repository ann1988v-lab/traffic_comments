"""Read-only planning, then explicit join/send execution of the saved plan."""
import asyncio
import csv
import io
import re
from urllib.parse import urlparse

from telethon import errors, functions, types, utils

from accounts import InputError, error_text


def parse_target(value):
    value = value.strip()
    if value.startswith("@") and re.fullmatch(r"@[A-Za-z][A-Za-z0-9_]{3,31}", value):
        return "public", value[1:]
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.netloc not in {"t.me", "telegram.me"}:
        raise InputError("Нужны @username либо ссылки https://t.me/...")
    parts = parsed.path.strip("/").split("/")
    if len(parts) == 2 and parts[0] == "addlist" and re.fullmatch(r"[\w-]+", parts[1]):
        return "folder", parts[1]
    if len(parts) == 1 and re.fullmatch(r"\+[A-Za-z0-9_-]+", parts[0]):
        return "private", parts[0][1:]
    if len(parts) == 2 and parts[0] == "joinchat" and re.fullmatch(r"[A-Za-z0-9_-]+", parts[1]):
        return "private", parts[1]
    if len(parts) == 1 and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", parts[0]):
        return "public", parts[0]
    raise InputError("Неизвестный формат ссылки. Ссылки на сообщения не подходят")


def parse_targets(text):
    result = []
    seen = set()
    lines = []
    for line in text.splitlines():
        links = re.findall(r"https://(?:t\.me|telegram\.me)/[^\s<>\[\]()]+", line)
        lines.extend(links or [line])
    for line in lines:
        if not line.strip():
            continue
        kind, value = parse_target(line)
        if (kind, value.lower() if kind == "public" else value) not in seen:
            result.append(line.strip())
            seen.add((kind, value.lower() if kind == "public" else value))
    if not result:
        raise InputError("Список чатов пуст")
    if len(result) > 300:
        raise InputError("За один план поддерживается до 300 исходных ссылок")
    return result


def pack(entity, joined, source, kind, value):
    if not isinstance(entity, (types.Channel, types.Chat, types.ChannelForbidden, types.ChatForbidden)):
        raise InputError("Ссылка ведёт на личный аккаунт, а не чат")
    if getattr(entity, "deactivated", False):
        raise InputError("Чат деактивирован; используйте ссылку на действующую группу")
    return {"id": utils.get_peer_id(entity), "raw_id": entity.id,
            "hash": getattr(entity, "access_hash", 0),
            "channel": isinstance(entity, (types.Channel, types.ChannelForbidden)),
            "title": entity.title, "joined": joined, "source": source,
            "kind": kind, "value": value,
            "broadcast": bool(getattr(entity, "broadcast", False)),
            "username": getattr(entity, "username", None)}


def input_peer(item):
    if item["channel"]:
        return types.InputPeerChannel(item["raw_id"], item["hash"])
    return types.InputPeerChat(item["raw_id"])


async def prepare(client, targets, issues=None):
    dialogs = {utils.get_peer_id(d.entity): d.entity async for d in client.iter_dialogs()}
    issues = issues if issues is not None else []
    result = []
    def issue(source, exc, chat_id=None, title=None):
        issues.append({"id": str(chat_id) if chat_id is not None else "source:" + source,
                       "title": title or source, "source": source, "reason": error_text(exc)})
    for source in targets:
        try:
            kind, value = parse_target(source)
            if kind == "folder":
                invite = await client(functions.chatlists.CheckChatlistInviteRequest(value))
                peers = (list(invite.peers) if hasattr(invite, "peers") else
                         list(invite.missing_peers) + list(invite.already_peers))
                by_id = {utils.get_peer_id(c): c for c in invite.chats}
                if not peers:
                    raise InputError("В папке нет доступных чатов")
                for peer in peers:
                    chat_id = utils.get_peer_id(peer)
                    entity = dialogs.get(chat_id) or by_id.get(chat_id)
                    try:
                        if isinstance(peer, types.PeerUser):
                            raise InputError("В папке указан личный аккаунт, он пропущен")
                        if entity is None:
                            entity = await client.get_entity(peer)
                        if utils.get_peer_id(entity) != chat_id:
                            raise InputError("Telegram не вернул данные нужного чата")
                        result.append(pack(entity, chat_id in dialogs, source, kind, value))
                    except errors.FloodWaitError:
                        raise
                    except Exception as exc:
                        issue(source, exc, chat_id, getattr(entity, "title", None))
            elif kind == "private":
                invite = await client(functions.messages.CheckChatInviteRequest(value))
                if isinstance(invite, types.ChatInviteAlready):
                    entity = dialogs.get(utils.get_peer_id(invite.chat)) or invite.chat
                    result.append(pack(entity, True, source, kind, value))
                else:
                    result.append({"id": "invite:" + value, "title": invite.title,
                                   "joined": False, "source": source, "kind": kind, "value": value})
            else:
                entity = await client.get_entity(value)
                result.append(pack(entity, utils.get_peer_id(entity) in dialogs, source, kind, value))
        except errors.FloodWaitError:
            raise
        except Exception as exc:
            issue(source, exc)
    unique = {}
    for item in result:
        unique.setdefault(str(item["id"]), item)
    return list(unique.values())


async def wait_or_stop(stop, seconds):
    if stop.is_set():
        raise asyncio.CancelledError()
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return
    raise asyncio.CancelledError()




def report_csv(store, job_id):
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["Задача", "Аккаунт", "Канал/пост", "Публикация", "Ссылка на пост", "Статус", "Причина"])
    def cell(value):
        text = str(value or "")
        return "'" + text if text.startswith(("=", "+", "-", "@", "\t", "\r")) else text
    for row in store.deliveries(job_id):
        writer.writerow([job_id, row["account"], row["chat"],
                         cell(row["title"]), cell(row["source"]), row["status"], cell(row["reason"])])
    return output.getvalue().encode("utf-8-sig")


class Runner:
    def __init__(self, store, notify, reporter=None):
        self.store = store
        self.notify = notify
        self.reporter = reporter
        self.task = None
        self.stop = asyncio.Event()
        self.job_id = None

    @property
    def busy(self):
        return self.task is not None and not self.task.done()

    def launch(self, job_id):
        if self.busy:
            raise InputError("Уже выполняется задача. Сначала остановите её")
        if self.store.job(job_id)["state"] == "done":
            raise InputError("Эта задача уже завершена. Создайте новый план")
        self.stop = asyncio.Event()
        self.job_id = job_id
        self.store.state(job_id, "running")
        self.task = asyncio.create_task(self.run(job_id))

    async def rpc(self, operation):
        while True:
            if self.stop.is_set():
                raise asyncio.CancelledError()
            try:
                return await operation()
            except errors.FloodWaitError as exc:
                seconds = max(1, int(exc.seconds))
                text = f"Telegram требует паузу {seconds} сек. Жду на текущем аккаунте. Остановка: /stop"
                self.store.state(self.job_id, "running", text)
                await self.safe_notify(text)
                await wait_or_stop(self.stop, seconds)





    async def safe_notify(self, text):
        try:
            await self.notify(text)
        except Exception:
            pass  # Progress is persisted; a Bot API outage must not trigger a resend.

    async def shutdown(self):
        if self.busy:
            self.stop.set()
            try:
                await asyncio.wait_for(asyncio.shield(self.task), timeout=25)
            except asyncio.TimeoutError:
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)

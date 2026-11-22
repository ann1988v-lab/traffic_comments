"""Channel comments addressed to the discussion root of each approved post."""
import asyncio
import io

from telethon import errors, functions, types, utils
from telethon.sessions import StringSession

from accounts import InputError, disconnect_after_check, error_text, make_client
from delivery import Runner as BaseRunner, input_peer, pack, prepare as prepare_targets, wait_or_stop


MODES = {"latest": "Последний пост", "recent": "Несколько последних", "watch": "Следить за новыми"}


def post_link(channel, post_id):
    if channel.get("username"):
        return f"https://t.me/{channel['username']}/{post_id}"
    return f"https://t.me/c/{channel['raw_id']}/{post_id}"


def group_posts(messages, count=None):
    groups = {}
    for message in sorted(messages, key=lambda m: m.id, reverse=True):
        if not isinstance(message, types.Message) or not message.post:
            continue
        key = f"album:{message.grouped_id}" if message.grouped_id else f"post:{message.id}"
        comments_available = bool(message.replies and message.replies.comments)
        if key not in groups:
            groups[key] = {"post_id": message.id, "post_key": key,
                           "comments_available": comments_available}
        elif comments_available and not groups[key]["comments_available"]:
            groups[key]["post_id"] = message.id
            groups[key]["comments_available"] = True
        elif comments_available == groups[key]["comments_available"]:
            groups[key]["post_id"] = min(groups[key]["post_id"], message.id)
    posts = list(groups.values())
    return posts if count is None else posts[:count]


def post_record(channel, post):
    return {**post, "id": f"{channel['raw_id']}/{post['post_id']}",
            "title": f"{channel['title']} · пост {post['post_id']}",
            "source": post_link(channel, post["post_id"])}


async def latest_posts(client, channel, count):
    messages = await client.get_messages(input_peer(channel), limit=count * 10 + 10)
    return group_posts(messages, count)


async def discussion(client, channel, post_id, dialogs, rpc=None):
    request = functions.messages.GetDiscussionMessageRequest(input_peer(channel), post_id)
    try:
        result = await rpc(lambda: client(request)) if rpc else await client(request)
    except errors.MsgIdInvalidError:
        raise InputError("Telegram не вернул ветку комментариев этого поста (MSG_ID_INVALID). "
                         "Пост или сообщение обсуждения могли быть удалены, либо комментарии недоступны.") from None
    roots = [message for message in result.messages
             if isinstance(message.peer_id, types.PeerChannel)
             and message.peer_id.channel_id != channel["raw_id"]]
    if not roots:
        raise InputError("У поста нет доступной ветки комментариев")
    root = min(roots, key=lambda message: message.id)
    group_id = utils.get_peer_id(root.peer_id)
    group = dialogs.get(group_id)
    if not isinstance(group, (types.Channel, types.ChannelForbidden)):
        group = next((chat for chat in result.chats if utils.get_peer_id(chat) == group_id), None)
    if not isinstance(group, (types.Channel, types.ChannelForbidden)):
        raise InputError("Telegram не вернул данные связанного чата обсуждений")
    if not getattr(group, "access_hash", None) or root.id <= 0:
        raise InputError("Недостаточно данных для ответа в ветке поста")
    packed = pack(group, group_id in dialogs, "", "public", getattr(group, "username", "") or "")
    return {"discussion": packed, "reply_id": root.id}


async def linked_discussion(client, channel, dialogs, rpc=None):
    request = functions.channels.GetFullChannelRequest(input_peer(channel))
    result = await rpc(lambda: client(request)) if rpc else await client(request)
    linked_id = result.full_chat.linked_chat_id
    if not linked_id:
        raise InputError("У канала не подключён чат обсуждений")
    group_id = utils.get_peer_id(types.PeerChannel(linked_id))
    group = dialogs.get(group_id)
    if not isinstance(group, (types.Channel, types.ChannelForbidden)):
        group = next((chat for chat in result.chats if chat.id == linked_id), None)
    if not isinstance(group, (types.Channel, types.ChannelForbidden)) or not getattr(group, "access_hash", None):
        raise InputError("Нет данных для вступления в связанный чат обсуждений")
    return pack(group, group_id in dialogs, "", "public", getattr(group, "username", "") or "")


async def prepare(client, targets, issues=None, mode="latest", post_count=3, prepared=None):
    if mode not in MODES:
        raise InputError("Выберите режим комментирования")
    issues = issues if issues is not None else []
    channels = await prepare_targets(client, targets, issues)
    dialogs = {utils.get_peer_id(d.entity): d.entity async for d in client.iter_dialogs()}
    result = prepared if prepared is not None else []
    for channel in channels:
        try:
            if not channel.get("broadcast"):
                raise InputError("Получатель является группой/чатом. Для комментариев под постами нужна ссылка на канал")
            channel["posts"] = []
            channel["joined"] = True
            if mode == "watch":
                group = await linked_discussion(client, channel, dialogs)
                channel["discussion_title"] = group["title"]
                channel["joined"] = group["joined"]
                result.append(channel)
            else:
                count = 1 if mode == "latest" else post_count
                posts = await latest_posts(client, channel, count)
                if not posts:
                    raise InputError("В канале нет доступных публикаций")
                result.append(channel)
                for post in posts:
                    record = post_record(channel, post)
                    try:
                        if not post["comments_available"]:
                            raise InputError("Telegram не указывает доступные комментарии у этого поста")
                        try:
                            record.update(await discussion(client, channel, post["post_id"], dialogs))
                        except (errors.UserNotParticipantError, errors.ChannelPrivateError):
                            record["discussion"] = await linked_discussion(client, channel, dialogs)
                            record["reply_id"] = None
                        channel["posts"].append(record)
                        channel["joined"] = all(p["discussion"]["joined"] for p in channel["posts"])
                    except errors.FloodWaitError:
                        raise
                    except Exception as exc:
                        issues.append({**record, "reason": error_text(exc)})
        except errors.FloodWaitError:
            raise
        except Exception as exc:
            issues.append({"id": str(channel["id"]), "title": channel["title"],
                           "source": channel["source"], "reason": error_text(exc)})
    return result


class Runner(BaseRunner):
    def __init__(self, store, notify, reporter=None):
        super().__init__(store, notify, reporter)
        self.last_action = None

    async def rpc(self, operation):
        async def bounded():
            async with asyncio.timeout(45):
                return await operation()
        return await super().rpc(bounded)

    async def pace(self, delay):
        loop = asyncio.get_running_loop()
        if self.last_action is not None:
            remaining = delay - (loop.time() - self.last_action)
            if remaining > 0:
                await wait_or_stop(self.stop, remaining)
        if self.stop.is_set():
            raise asyncio.CancelledError()
        self.last_action = loop.time()

    async def send(self, client, item, snapshot):
        group = item["discussion"]
        reply_id = item["reply_id"]
        if reply_id <= 0:
            raise InputError("Не определена ветка комментариев поста")
        peer = input_peer(group)
        media = snapshot.get("media")
        if not media:
            return await client.send_message(peer, snapshot["message"], reply_to=reply_id,
                                             parse_mode=None, link_preview=False)
        with io.BytesIO(self.store.read_media(media["id"])) as file:
            file.name = media["name"]
            kwargs = {}
            if media["kind"] == "video":
                kwargs["mime_type"] = media.get("mime_type") or "video/mp4"
                kwargs["supports_streaming"] = kwargs["mime_type"] == "video/mp4"
                kwargs["attributes"] = [types.DocumentAttributeVideo(
                    duration=media.get("duration") or 0, w=media.get("width") or 0,
                    h=media.get("height") or 0, supports_streaming=kwargs["supports_streaming"])]
            return await client.send_file(peer, file, caption=snapshot["message"], reply_to=reply_id,
                                          parse_mode=None, force_document=False, **kwargs)

    def summary(self, job_id):
        rows = self.store.deliveries(job_id)
        counts = {status: sum(r["status"] == status for r in rows)
                  for status in ("sent", "failed", "pending", "skipped")}
        return (f"Комментариев отправлено: {counts['sent']}. Ошибок: {counts['failed']}. "
                f"Требует проверки: {counts['pending']}. Уже обработано ранее: {counts['skipped']}.")

    def record_failure(self, job, account, item, exc):
        self.store.failed(job, 0, account, item, error_text(exc))

    async def join_discussion(self, client, group, dialogs, delay):
        if group["id"] in dialogs:
            return
        await self.pace(delay)
        try:
            await self.rpc(lambda: client(functions.channels.JoinChannelRequest(input_peer(group))))
        except errors.UserAlreadyParticipantError:
            pass
        dialogs[group["id"]] = None

    async def process_post(self, client, job, account, channel, post, snapshot, dialogs):
        item = post_record(channel, post)
        if self.store.recorded(job, 0, account, item["id"]):
            return
        if post.get("comments_available") is False:
            self.record_failure(job, account, item,
                                InputError("Telegram не указывает доступные комментарии у этого поста"))
            return
        try:
            # Resolve again before sending in case the discussion changed since preview.
            try:
                thread = await discussion(client, channel, post["post_id"], dialogs, self.rpc)
            except (errors.UserNotParticipantError, errors.ChannelPrivateError):
                group = await linked_discussion(client, channel, dialogs, self.rpc)
                await self.join_discussion(client, group, dialogs, snapshot["delay"])
                thread = await discussion(client, channel, post["post_id"], dialogs, self.rpc)
            item.update(thread)
            group = item["discussion"]
            await self.join_discussion(client, group, dialogs, snapshot["delay"])
            await self.pace(snapshot["delay"])
        except errors.PeerFloodError:
            raise
        except Exception as exc:
            self.record_failure(job, account, item, exc)
            return
        if not self.store.claim_comment(account, channel["raw_id"], post["post_key"], job):
            self.store.skipped(job, account, item, "Для этого аккаунта пост уже обработан или ожидает проверки")
            return
        self.store.claim(job, 0, account, item["id"], item["title"], item["source"])
        try:
            await self.rpc(lambda: self.send(client, item, snapshot))
        except BaseException as exc:
            uncertain = not isinstance(exc, (errors.RPCError, InputError)) or isinstance(exc, errors.ServerError)
            if not uncertain:
                self.store.comment_status(account, channel["raw_id"], post["post_key"], "failed")
            if isinstance(exc, Exception):
                self.store.failed(job, 0, account, item, error_text(exc), uncertain)
            if isinstance(exc, (asyncio.CancelledError, errors.PeerFloodError)):
                raise
            if not isinstance(exc, Exception):
                raise
            return
        self.store.comment_status(account, channel["raw_id"], post["post_key"], "sent")
        self.store.sent(job, 0, account, item["id"])
        self.store.state(job, "running", self.summary(job))

    async def watch_channel(self, client, job, account, channel, snapshot, dialogs, initialize=False):
        cursor = self.store.cursor(job, account, channel["raw_id"])
        if cursor is None:
            latest = await self.rpc(lambda: client.get_messages(input_peer(channel), limit=1))
            self.store.set_cursor(job, account, channel["raw_id"], latest[0].id if latest else 0)
            return
        async def read_new():
            return [message async for message in client.iter_messages(
                input_peer(channel), min_id=cursor, reverse=True, limit=100)]
        messages = await self.rpc(read_new)
        for post in sorted(group_posts(messages), key=lambda p: p["post_id"]):
            await self.process_post(client, job, account, channel, post, snapshot, dialogs)
        if messages:
            self.store.set_cursor(job, account, channel["raw_id"], max(m.id for m in messages))

    async def account_pass(self, job, account_plan, snapshot, initialize=False):
        if self.stop.is_set():
            raise asyncio.CancelledError()
        account = account_plan["account_id"]
        client = None
        try:
            payload = self.store.account(account)
            client = make_client(StringSession(payload["session"]), payload["options"])
            async with asyncio.timeout(30):
                await client.connect()
            if not await self.rpc(client.is_user_authorized):
                raise InputError("Сессия отозвана. Переподключите аккаунт")
            async def read_dialogs():
                return {utils.get_peer_id(d.entity): d.entity async for d in client.iter_dialogs()}
            dialogs = await self.rpc(read_dialogs)
            for channel in account_plan["items"]:
                if self.stop.is_set():
                    raise asyncio.CancelledError()
                try:
                    if snapshot["mode"] == "watch":
                        await self.watch_channel(client, job, account, channel, snapshot, dialogs, initialize)
                    else:
                        for post in reversed(channel["posts"]):
                            await self.process_post(client, job, account, channel, post, snapshot, dialogs)
                except errors.PeerFloodError:
                    raise
                except Exception as exc:
                    issue = {"id": "channel:" + str(channel["raw_id"]), "title": channel["title"],
                             "source": channel["source"]}
                    self.record_failure(job, account, issue, exc)
        except (errors.PeerFloodError, asyncio.CancelledError):
            raise
        except Exception as exc:
            issue = {"id": "account:" + str(account), "title": "Аккаунт " + str(account), "source": ""}
            self.record_failure(job, account, issue, exc)
        finally:
            if client:
                await disconnect_after_check(client)

    async def run(self, job_id):
        self.last_action = None
        try:
            snapshot = self.store.job(job_id)["snapshot"]
            for account_plan in snapshot["accounts"]:
                for issue in account_plan.get("issues", []):
                    if not self.store.recorded(job_id, 0, account_plan["account_id"], issue["id"]):
                        self.store.failed(job_id, 0, account_plan["account_id"], issue, issue["reason"])
            if snapshot["mode"] == "watch":
                # Save all baselines before the first polling pass.
                for account_plan in snapshot["accounts"]:
                    await self.account_pass(job_id, account_plan, snapshot, initialize=True)
                await self.safe_notify(f"Наблюдение {job_id} запущено. Проверка каждые {snapshot['poll_seconds']} сек.")
                while True:
                    await wait_or_stop(self.stop, snapshot["poll_seconds"])
                    for account_plan in snapshot["accounts"]:
                        await self.account_pass(job_id, account_plan, snapshot)
                    self.store.state(job_id, "running", "Наблюдение за новыми постами. " + self.summary(job_id))
            else:
                for account_plan in snapshot["accounts"]:
                    await self.account_pass(job_id, account_plan, snapshot)
                self.store.progress(job_id, 1)
                self.store.state(job_id, "done", self.summary(job_id))
        except asyncio.CancelledError:
            self.store.state(job_id, "stopped", self.summary(job_id))
        except Exception as exc:
            self.store.state(job_id, "error", error_text(exc))
        finally:
            await self.finish_report(job_id)

    async def finish_report(self, job_id):
        job = self.store.job(job_id)
        labels = {"done": "завершена", "stopped": "остановлена", "error": "прервана"}
        lines = [f"Задача {job_id} {labels.get(job['state'], job['state'])}.", self.summary(job_id)]
        if job["state"] == "error":
            lines.append(job["detail"])
        failures = [row for row in self.store.deliveries(job_id) if row["status"] in {"failed", "pending"}]
        lines.extend(f"• {row['title'][:80]} — {row['reason'][:180] or 'Нет подтверждения отправки'}"
                     for row in failures[:10])
        if len(failures) > 10:
            lines.append("Остальные ошибки указаны в CSV.")
        await self.safe_notify("\n".join(lines)[:3900])
        if self.reporter:
            try:
                await self.reporter(job_id)
            except Exception:
                pass

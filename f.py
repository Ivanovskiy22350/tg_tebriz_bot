import logging
import uuid
import sqlite3
from datetime import datetime, timedelta, timezone
import re
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
    PicklePersistence
)
from telegram.constants import ParseMode

# ВАШ ТОКЕН БОТА - ПОЖАЛУЙСТА, ОТОЗОВИТЕ ЕГО И СГЕНЕРИРУЙТЕ НОВЫЙ!
TOKEN = "8036227115:AAEsS0lvg5K9H5oWQrQ44OMvuXi3YCjHjac"
DB_NAME = "bot_database.db"
PERSISTENCE_FILE = "bot_persistence.pickle"
ADMIN_PASSWORD = "!6c8cO#^zVc_MGG8G*j#Tfe0!N23G_"
ADMIN_USER_IDS = [6354749818, 7204326924, 5293475879]

MAINTENANCE_KEY = "maintenance_mode_active"

WAITING_PASSWORD = 0
ASK_TICKET_MESSAGE, CONFIRM_TICKET_SEND = range(1, 3)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def escape_markdown_v2(text: str) -> str:
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return "".join(f'\\{char}' if char in escape_chars else char for char in text)


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, reputation INTEGER DEFAULT 0)")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS fines (fine_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, issuer_id INTEGER NOT NULL, amount INTEGER NOT NULL, due_date_ts REAL NOT NULL, payment_code TEXT UNIQUE NOT NULL, is_paid INTEGER DEFAULT 0, created_at_ts REAL NOT NULL, FOREIGN KEY (user_id) REFERENCES users (user_id), FOREIGN KEY (issuer_id) REFERENCES users (user_id))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS mailbox (mail_id TEXT PRIMARY KEY, recipient_user_id INTEGER NOT NULL, sender_user_id INTEGER, sender_display_name TEXT DEFAULT 'Администрация', subject TEXT NOT NULL, body TEXT NOT NULL, timestamp_sent_ts REAL NOT NULL, is_read INTEGER DEFAULT 0, FOREIGN KEY (recipient_user_id) REFERENCES users (user_id), FOREIGN KEY (sender_user_id) REFERENCES users (user_id))")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS support_tickets (ticket_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, user_message TEXT NOT NULL, timestamp_created_ts REAL NOT NULL, status TEXT DEFAULT 'open', assigned_admin_id INTEGER, admin_reply TEXT, timestamp_admin_replied_ts REAL, FOREIGN KEY (user_id) REFERENCES users (user_id), FOREIGN KEY (assigned_admin_id) REFERENCES users (user_id))")
    conn.commit()
    conn.close()
    logger.info("База данных инициализирована.")


def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_user_in_db(user_id: int, username: str | None) -> None:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        db_user = cursor.fetchone()
        if db_user is None:
            cursor.execute("INSERT INTO users (user_id, username, reputation) VALUES (?, ?, 0)",
                           (user_id, username)); logger.info(
                f"Новый пользователь {user_id} (@{username}) добавлен в БД.")
        elif username and db_user["username"] != username:
            cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id)); logger.info(
                f"Username для {user_id} обновлен на @{username}.")
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД ensure_user_in_db для {user_id}: {e}")
    finally:
        conn.close()


def get_user_display_name_from_db(user_id: int, default_if_not_found=True) -> str:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
        user_row = cursor.fetchone()
        if user_row and user_row["username"]: return f"@{user_row['username']}"
        if default_if_not_found: return f"пользователю с ID {user_id}"
        return ""
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД display_name для {user_id}: {e}")
        if default_if_not_found: return f"пользователю с ID {user_id}"
        return ""
    finally:
        conn.close()


def get_user_id_by_username(username_to_find: str) -> int | None:
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_id FROM users WHERE username = ?",
                       (username_to_find,)); user_row = cursor.fetchone(); return user_row[
            "user_id"] if user_row else None
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД user_id по username @{username_to_find}: {e}"); return None
    finally:
        conn.close()


def generate_short_id(length=5) -> str: return uuid.uuid4().hex[:length].upper()


def generate_unique_code(length=8) -> str: return uuid.uuid4().hex[:length].upper()


def count_unread_mail(user_id: int) -> int:
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) as count FROM mailbox WHERE recipient_user_id = ? AND is_read = 0",
                       (user_id,)); count_row = cursor.fetchone(); return count_row['count'] if count_row else 0
    except sqlite3.Error as e:
        logger.error(f"Ошибка БД непрочитанных писем для {user_id}: {e}"); return 0
    finally:
        conn.close()


async def check_maintenance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if context.bot_data.get(MAINTENANCE_KEY, False):
        if update.message:
            await update.message.reply_text(
                "⚙️ Бот на тех. работах.\nПоддержка: @Ivanovskiy22350 @fyflik567 @Dark_prince_gg")
        elif update.callback_query:
            await update.callback_query.answer("⚙️ Бот на тех. работах.", show_alert=True)
        return True
    return False


def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    unread_count = count_unread_mail(user_id)
    mail_button_text = "📬 Почта" + (f" ({unread_count} новых)" if unread_count > 0 else "")
    keyboard_buttons = [
        [InlineKeyboardButton("🏅 Моя Репутация", callback_data='show_reputation')],
        [InlineKeyboardButton(mail_button_text, callback_data='show_mailbox_main')],
        [InlineKeyboardButton("📞 Поддержка", callback_data='support_menu')],
        [InlineKeyboardButton("📜 Мои Штрафы", callback_data='show_fines')], ]
    return InlineKeyboardMarkup(keyboard_buttons)


def get_reputation_view_keyboard() -> InlineKeyboardMarkup: keyboard = [
    [InlineKeyboardButton("⬅️ Назад", callback_data='main_menu_nav')]]; return InlineKeyboardMarkup(keyboard)


def get_fines_view_keyboard() -> InlineKeyboardMarkup: keyboard = [
    [InlineKeyboardButton("⬅️ Назад", callback_data='main_menu_nav')]]; return InlineKeyboardMarkup(keyboard)


def get_mailbox_keyboard(mails: list, user_id: int) -> InlineKeyboardMarkup:
    keyboard = []
    if mails:
        for mail in mails: subject_preview = mail['subject'][:30] + (
            "..." if len(mail['subject']) > 30 else ""); read_status = "🆕 " if not mail[
            'is_read'] else "▫️ "; keyboard.append(
            [InlineKeyboardButton(f"{read_status}{subject_preview} (от {mail['sender_display_name']})",
                                  callback_data=f"read_mail_{mail['mail_id']}")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад в Главное Меню", callback_data='main_menu_nav')]);
    return InlineKeyboardMarkup(keyboard)


def get_read_mail_keyboard(mail_id: str) -> InlineKeyboardMarkup: keyboard = [
    [InlineKeyboardButton("⬅️ Назад к письмам", callback_data='show_mailbox_main')]]; return InlineKeyboardMarkup(
    keyboard)


def get_support_menu_keyboard(user_is_admin: bool) -> InlineKeyboardMarkup:
    keyboard_list = [[InlineKeyboardButton("💬 Создать обращение", callback_data='create_ticket_entry')],
                     [InlineKeyboardButton("📑 Мои обращения", callback_data='my_tickets')], ]
    if user_is_admin: keyboard_list.insert(1, [InlineKeyboardButton("👁️‍🗨️ Все обращения (Админ)",
                                                                    callback_data='view_all_tickets')])
    keyboard_list.append([InlineKeyboardButton("⬅️ Назад в Главное Меню", callback_data='main_menu_nav')]);
    return InlineKeyboardMarkup(keyboard_list)


def get_confirm_ticket_keyboard() -> InlineKeyboardMarkup: keyboard = [
    [InlineKeyboardButton("✅ Отправить", callback_data='send_confirmed_ticket')],
    [InlineKeyboardButton("❌ Отмена", callback_data='cancel_ticket_creation')]]; return InlineKeyboardMarkup(keyboard)


async def _actual_add_reputation_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE,
                                       args: list) -> None:
    if len(args) != 2: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                      text="⚠️ Формат: /add <ID/@user> <кол-во>"); return
    target_identifier = args[0];
    amount_str = args[1]
    try:
        amount = int(amount_str)
    except ValueError:
        await context.bot.send_message(chat_id=original_update_info['chat_id'], text="⚠️ Кол-во - число."); return
    target_user_id = None
    if target_identifier.startswith('@'):
        username_to_find = target_identifier[1:]; target_user_id = get_user_id_by_username(username_to_find);
    else:
        try:
            target_user_id = int(target_identifier)
        except ValueError:
            await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                           text="⚠️ ID - число или @username."); return
    if not target_user_id and target_identifier.startswith('@'): await context.bot.send_message(
        chat_id=original_update_info['chat_id'], text=f"ℹ️ @{username_to_find} не найден."); return
    ensure_user_in_db(target_user_id, None)
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE users SET reputation = reputation + ? WHERE user_id = ?",
                       (amount, target_user_id)); conn.commit(); cursor.execute(
            "SELECT reputation FROM users WHERE user_id = ?",
            (target_user_id,)); new_rep_row = cursor.fetchone(); new_rep = new_rep_row[
            "reputation"] if new_rep_row else "неизвестно"
    except sqlite3.Error as e:
        logger.error(f"БД реп {target_user_id}: {e}"); await context.bot.send_message(
            chat_id=original_update_info['chat_id'], text="❌ Ошибка БД реп."); return
    finally:
        conn.close()
    target_display_name = get_user_display_name_from_db(target_user_id);
    issuer_display_name = get_user_display_name_from_db(original_update_info['user_id'])
    logger.info(f"{issuer_display_name} реп для {target_display_name} на {amount}. Новая: {new_rep}")
    await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                   text=f"✅ Реп для {target_display_name} изм. на {amount}.\nТек: {new_rep}")


async def _actual_fine_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE, args: list) -> None:
    issuer_id = original_update_info['user_id']
    if len(args) != 3: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                      text="⚠️ Формат: /fine <сумма> <ID/@user> <дни>"); return
    try:
        fine_amount = int(args[0]); days_to_pay = int(args[2]);
    except ValueError:
        await context.bot.send_message(chat_id=original_update_info['chat_id'], text=f"⚠️ Ошибка суммы/дней."); return
    if fine_amount <= 0 or days_to_pay <= 0: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                                            text=f"⚠️ Сумма/дни > 0."); return
    target_identifier = args[1];
    target_user_id = None
    if target_identifier.startswith('@'):
        username_to_find = target_identifier[1:]; target_user_id = get_user_id_by_username(username_to_find);
    else:
        try:
            target_user_id = int(target_identifier)
        except ValueError:
            await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                           text="⚠️ ID - число или @username."); return
    if not target_user_id and target_identifier.startswith('@'): await context.bot.send_message(
        chat_id=original_update_info['chat_id'], text=f"ℹ️ @{username_to_find} не найден."); return
    ensure_user_in_db(target_user_id, None)
    due_date = datetime.now(timezone.utc) + timedelta(days=days_to_pay);
    fine_id_uuid = str(uuid.uuid4());
    payment_code = generate_unique_code();
    created_at_ts = datetime.now(timezone.utc).timestamp()
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO fines VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                       (fine_id_uuid, target_user_id, issuer_id, fine_amount, due_date.timestamp(), payment_code,
                        created_at_ts)); conn.commit()
    except sqlite3.Error as e:
        logger.error(f"БД штраф {target_user_id}: {e}"); await context.bot.send_message(
            chat_id=original_update_info['chat_id'], text="❌ Ошибка БД штрафа."); return
    finally:
        conn.close()
    target_display_name = get_user_display_name_from_db(target_user_id);
    issuer_display_name = get_user_display_name_from_db(issuer_id)
    logger.info(
        f"{issuer_display_name} штраф {fine_id_uuid} {fine_amount} to {target_display_name} до {due_date.strftime('%Y-%m-%d')}. Код: {payment_code}")
    await context.bot.send_message(chat_id=original_update_info['chat_id'], text=(
        f"✅ Штраф {fine_amount} выписан {target_display_name}.\nСрок: {days_to_pay} д. (Код: {payment_code})"))
    try:
        due_date_str = due_date.strftime('%d.%m.%Y %H:%M %Z'); message_to_target = (
            f"🔔 Вам выписан штраф!\nСумма: {fine_amount}\nОплатить до: {due_date_str}\nВыписал: {issuer_display_name}\nДля урегулирования обратитесь к администрации."); await context.bot.send_message(
            chat_id=target_user_id, text=message_to_target, parse_mode=ParseMode.HTML); logger.info(
            f"Уведомление о штрафе {target_user_id}")
    except Exception as e:
        logger.error(f"Не уведомить о штрафе {target_user_id}: {e}"); await context.bot.send_message(
            chat_id=original_update_info['chat_id'], text=(f"⚠️ Не удалось уведомить {target_display_name}."))


async def _actual_delfine_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE, args: list) -> None:
    deleter_id = original_update_info['user_id']
    if len(args) != 1: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                      text="⚠️ Формат: /delfine <ID/@user>"); return
    target_identifier = args[0];
    target_user_id = None
    if target_identifier.startswith('@'):
        username_to_find = target_identifier[1:]; target_user_id = get_user_id_by_username(username_to_find);
    else:
        try:
            target_user_id = int(target_identifier)
        except ValueError:
            await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                           text="⚠️ ID - число или @username."); return
    if not target_user_id and target_identifier.startswith('@'): await context.bot.send_message(
        chat_id=original_update_info['chat_id'], text=f"ℹ️ @{username_to_find} не найден."); return
    conn_check = get_db_connection();
    cursor_check = conn_check.cursor();
    cursor_check.execute("SELECT user_id FROM users WHERE user_id = ?", (target_user_id,));
    target_exists = cursor_check.fetchone();
    conn_check.close()
    if not target_exists: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                         text=f"ℹ️ ID {target_user_id} не найден (д /start)."); return
    target_display_name = get_user_display_name_from_db(target_user_id)
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM fines WHERE user_id = ? AND is_paid = 0",
                       (target_user_id,)); deleted_rows_count = cursor.rowcount; conn.commit(); deleter_display_name = get_user_display_name_from_db(
            deleter_id)
    except sqlite3.Error as e:
        logger.error(f"БД удал. штрафов {target_user_id}: {e}"); await context.bot.send_message(
            chat_id=original_update_info['chat_id'], text="❌ Ошибка БД удал. штрафов."); return
    finally:
        conn.close()
    if deleted_rows_count > 0:
        logger.info(f"{deleter_display_name} удалил {deleted_rows_count} штрафов для {target_display_name}.")
        await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                       text=f"✅ {deleted_rows_count} акт. штрафа(ов) для {target_display_name} удалены.")
        try:
            await context.bot.send_message(chat_id=target_user_id,
                                           text=f"🔔 Ваши акт. штрафы аннулированы админом {deleter_display_name}.")
        except Exception as e:
            logger.warning(f"Не уведомить {target_display_name} об удал. штрафов: {e}")
    else:
        await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                       text=f"ℹ️ У {target_display_name} нет акт. штрафов.")


async def _actual_sendmail_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE, args: list) -> None:
    issuer_id = original_update_info['user_id']
    if len(args) < 2: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                     text="⚠️ Формат: /sendmail <ID/@user> <Заголовок>;<Текст>"); return
    recipient_identifier = args[0];
    mail_content_combined = " ".join(args[1:])
    if ';' not in mail_content_combined: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                                        text="⚠️ Ошибка: Заголовок;Текст"); return
    subject, body = mail_content_combined.split(';', 1);
    subject = subject.strip();
    body = body.strip()
    if not subject or not body: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                               text="⚠️ Заголовок и тело не пустые."); return
    recipient_user_id = None
    if recipient_identifier.startswith('@'):
        username_to_find = recipient_identifier[1:]; recipient_user_id = get_user_id_by_username(username_to_find);
    else:
        try:
            recipient_user_id = int(
                recipient_identifier); conn_check = get_db_connection(); cursor_check = conn_check.cursor(); cursor_check.execute(
                "SELECT user_id FROM users WHERE user_id = ?", (recipient_user_id,));
        except ValueError:
            await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                           text="⚠️ ID - число или @username."); return
        if not cursor_check.fetchone(): await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                                       text=f"ℹ️ ID {recipient_user_id} не найден (д /start)."); conn_check.close(); return
        conn_check.close()
    if not recipient_user_id and recipient_identifier.startswith('@'): await context.bot.send_message(
        chat_id=original_update_info['chat_id'], text=f"ℹ️ @{username_to_find} не найден."); return
    ensure_user_in_db(recipient_user_id, None)
    mail_id = str(uuid.uuid4());
    timestamp_sent_ts = datetime.now(timezone.utc).timestamp();
    sender_display_name = get_user_display_name_from_db(issuer_id) or "Администрация"
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO mailbox VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                       (mail_id, recipient_user_id, issuer_id, sender_display_name, subject, body,
                        timestamp_sent_ts)); conn.commit(); logger.info(
            f"{sender_display_name} ({issuer_id}) письмо {mail_id} -> {recipient_user_id}.")
    except sqlite3.Error as e:
        logger.error(f"БД письма: {e}"); await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                                        text="❌ Ошибка БД письма."); return
    finally:
        conn.close()
    await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                   text=f"✅ Письмо '{subject}' отпр. {get_user_display_name_from_db(recipient_user_id)}.")
    try:
        await context.bot.send_message(chat_id=recipient_user_id,
                                       text=f"📬 Новое письмо от {sender_display_name}: '{subject}'.\nПроверьте Почту.")
    except Exception as e:
        logger.warning(f"Не уведомить о письме {recipient_user_id}: {e}")


async def _actual_replyticket_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE, args: list) -> None:
    admin_id = original_update_info['user_id']
    if len(args) < 2: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                     text="⚠️ Формат: /replyticket <ID_тикета> <Текст>"); return
    ticket_id_to_reply = args[0];
    reply_text = " ".join(args[1:])
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT user_id, status, assigned_admin_id FROM support_tickets WHERE ticket_id = ?",
                       (ticket_id_to_reply,)); ticket_data = cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"БД ответа {ticket_id_to_reply}: {e}"); await context.bot.send_message(
            chat_id=original_update_info['chat_id'], text="❌ Ошибка БД ответа."); return  # Важно закрыть conn в finally
    if not ticket_data: await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                       text=f"❌ Тикет '{ticket_id_to_reply}' не найден."); conn.close(); return
    if ticket_data['status'] == 'admin_replied' or ticket_data['status'] == 'closed': await context.bot.send_message(
        chat_id=original_update_info['chat_id'],
        text=f"ℹ️ Тикет '{ticket_id_to_reply}' уже отвечен/закрыт."); conn.close(); return
    if ticket_data['assigned_admin_id'] and ticket_data[
        'assigned_admin_id'] != admin_id: other_admin_name = get_user_display_name_from_db(
        ticket_data['assigned_admin_id']); await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                                                          text=f"ℹ️ Тикет '{ticket_id_to_reply}' взят {other_admin_name}."); conn.close(); return
    current_assigned_admin = ticket_data['assigned_admin_id'] if ticket_data['assigned_admin_id'] else admin_id
    timestamp_replied = datetime.now(timezone.utc).timestamp()
    try:
        cursor.execute(
            "UPDATE support_tickets SET admin_reply = ?, timestamp_admin_replied_ts = ?, status = 'admin_replied', assigned_admin_id = ? WHERE ticket_id = ?",
            (reply_text, timestamp_replied, current_assigned_admin, ticket_id_to_reply)); conn.commit()
    except sqlite3.Error as e:
        logger.error(f"БД ответа на тикет {ticket_id_to_reply}: {e}"); await context.bot.send_message(
            chat_id=original_update_info['chat_id'],
            text="❌ Ошибка БД ответа на тикет."); conn.close(); return  # Закрываем conn здесь тоже
    # conn.close() # Это соединение закроется в finally
    admin_display_name = get_user_display_name_from_db(admin_id)
    logger.info(f"{admin_display_name} ({admin_id}) ответил на тикет {ticket_id_to_reply}.")
    await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                   text=f"✅ Ответ на тикет {ticket_id_to_reply} отпр.")
    user_to_notify_id = ticket_data['user_id']
    try:
        user_ticket_message = (
            f" caseworker️ Админ {admin_display_name} ответил на обращение #{ticket_id_to_reply}:\n\n<i>{reply_text}</i>\n\nПроверьте 'Мои обращения'."); await context.bot.send_message(
            chat_id=user_to_notify_id, text=user_ticket_message, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Не уведомить {user_to_notify_id} об ответе {ticket_id_to_reply}: {e}")
        # conn_fallback = get_db_connection(); cursor_fallback = conn_fallback.cursor() # Используем основное соединение, если оно еще открыто
        try:
            mail_subject = f"Ответ на обращение #{ticket_id_to_reply}";
            mail_body = f"Админ {admin_display_name} ответил:\n\n{reply_text}"
            mail_id_fallback = str(uuid.uuid4());
            ts_fallback = datetime.now(timezone.utc).timestamp()
            cursor.execute("INSERT INTO mailbox VALUES (?, ?, ?, ?, ?, ?, ?, 0)",
                           (mail_id_fallback, user_to_notify_id, admin_id, admin_display_name, mail_subject, mail_body,
                            ts_fallback));
            conn.commit()  # Используем тот же cursor
            logger.info(f"Ответ {ticket_id_to_reply} отпр. {user_to_notify_id} через почту (fallback).")
        except sqlite3.Error as e_fb:
            logger.error(f"БД fallback письма: {e_fb}")
        # finally: conn_fallback.close() # Не нужно, если используем основной conn
    finally:
        conn.close()


async def _actual_tex_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE, args: list) -> None:
    context.bot_data[MAINTENANCE_KEY] = True
    await context.bot.send_message(chat_id=original_update_info['chat_id'], text="⚙️ Режим технических работ ВКЛЮЧЕН.")
    logger.info(f"Администратор {original_update_info['user_id']} включил режим тех. работ.")
    message_to_users = "⚙️ Бот уходит на технические работы и не будет работать определённое время.\nПоддержка: @Ivanovskiy22350 @fyflik567 @Dark_prince_gg"
    conn = get_db_connection();
    cursor = conn.cursor();
    cursor.execute("SELECT user_id FROM users");
    all_user_ids = [row['user_id'] for row in cursor.fetchall()];
    conn.close()
    sent_count = 0;
    failed_count = 0
    for user_id in all_user_ids:
        try:
            await context.bot.send_message(chat_id=user_id,
                                           text=message_to_users); sent_count += 1; await asyncio.sleep(0.1)
        except Exception as e:
            logger.warning(f"Не отпр. уведомл. о тех. работах {user_id}: {e}"); failed_count += 1
    await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                   text=f"Уведомл. о тех. работах отпр. {sent_count} польз. (ошибок: {failed_count}).")


async def _actual_texupd_logic(original_update_info: dict, context: ContextTypes.DEFAULT_TYPE, args: list) -> None:
    context.bot_data[MAINTENANCE_KEY] = False
    await context.bot.send_message(chat_id=original_update_info['chat_id'], text="✅ Режим технических работ ВЫКЛЮЧЕН.")
    logger.info(f"Администратор {original_update_info['user_id']} выключил режим тех. работ.")
    message_to_users = "✅ Бот снова стабильно работает, пользуйтесь!"
    conn = get_db_connection();
    cursor = conn.cursor();
    cursor.execute("SELECT user_id FROM users");
    all_user_ids = [row['user_id'] for row in cursor.fetchall()];
    conn.close()
    sent_count = 0;
    failed_count = 0
    for user_id in all_user_ids:
        try:
            await context.bot.send_message(chat_id=user_id,
                                           text=message_to_users); sent_count += 1; await asyncio.sleep(0.1)
        except Exception as e:
            logger.warning(f"Не отпр. уведомл. о возобнов. {user_id}: {e}"); failed_count += 1
    await context.bot.send_message(chat_id=original_update_info['chat_id'],
                                   text=f"Уведомл. о возобнов. отпр. {sent_count} польз. (ошибок: {failed_count}).")


async def protected_command_entry(update: Update, context: ContextTypes.DEFAULT_TYPE, command_name: str) -> int:
    if not update.message or not update.effective_user: return ConversationHandler.END
    if command_name != 'texupd' and await check_maintenance(update, context): return ConversationHandler.END
    context.user_data['pending_command_args'] = context.args;
    context.user_data['pending_command_name'] = command_name
    context.user_data['original_update_info'] = {'user_id': update.effective_user.id,
                                                 'username': update.effective_user.username,
                                                 'chat_id': update.message.chat_id}
    await update.message.reply_text("🔑 Введите пароль администратора:")
    return WAITING_PASSWORD


async def add_command_password_entry(update: Update,
                                     context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'add')


async def fine_command_password_entry(update: Update,
                                      context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'fine')


async def delfine_command_password_entry(update: Update,
                                         context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'delfine')


async def sendmail_command_password_entry(update: Update,
                                          context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'sendmail')


async def replyticket_command_password_entry(update: Update,
                                             context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'replyticket')


async def tex_command_password_entry(update: Update,
                                     context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'tex')


async def texupd_command_password_entry(update: Update,
                                        context: ContextTypes.DEFAULT_TYPE) -> int: return await protected_command_entry(
    update, context, 'texupd')


async def receive_password_and_execute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text: await update.message.reply_text(
        "Введите пароль текстом."); return WAITING_PASSWORD
    password_attempt = update.message.text
    original_update_info = context.user_data.get('original_update_info');
    pending_args = context.user_data.get('pending_command_args');
    pending_command = context.user_data.get('pending_command_name')
    for key in ['original_update_info', 'pending_command_args', 'pending_command_name']:
        if key in context.user_data: del context.user_data[key]
    if not original_update_info or pending_args is None or not pending_command: logger.error(
        "Нет данных user_data"); await update.message.reply_text(
        "Ошибка, попробуйте снова."); return ConversationHandler.END
    if password_attempt == ADMIN_PASSWORD:
        await update.message.reply_text("🔑 Пароль принят. Выполняю...")
        if pending_command == 'add':
            await _actual_add_reputation_logic(original_update_info, context, pending_args)
        elif pending_command == 'fine':
            await _actual_fine_logic(original_update_info, context, pending_args)
        elif pending_command == 'delfine':
            await _actual_delfine_logic(original_update_info, context, pending_args)
        elif pending_command == 'sendmail':
            await _actual_sendmail_logic(original_update_info, context, pending_args)
        elif pending_command == 'replyticket':
            await _actual_replyticket_logic(original_update_info, context, pending_args)
        elif pending_command == 'tex':
            await _actual_tex_logic(original_update_info, context, pending_args)
        elif pending_command == 'texupd':
            await _actual_texupd_logic(original_update_info, context, pending_args)
        else:
            logger.error(f"Неизв. команда: {pending_command}"); await update.message.reply_text("Внутр. ошибка.")
    else:
        await update.message.reply_text("⛔ Неверный пароль. Отменено.")
    return ConversationHandler.END


async def cancel_password_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return ConversationHandler.END
    for key in ['original_update_info', 'pending_command_args', 'pending_command_name']:
        if key in context.user_data: del context.user_data[key]
    await update.message.reply_text('Ввод пароля отменен.')
    return ConversationHandler.END


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context) and update.effective_user.id not in ADMIN_USER_IDS: return
    user = update.effective_user
    if not user:
        if update.message: await update.message.reply_text("Не удалось получить инфо."); return
    ensure_user_in_db(user.id, user.username)
    logger.info(f"User {user.id} (@{user.username}) /start")
    if update.message:
        await update.message.reply_text(
            "👋 Привет! Я твой бот.\n\n"
            "Команды админа (требуют пароль):\n"
            "/add, /fine, /delfine, /sendmail, /replyticket, /tex, /texupd\n"
            "/cancel - отменить ввод пароля",
            reply_markup=get_main_menu_keyboard(user.id)
        )


async def show_mailbox_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    conn = get_db_connection();
    cursor = conn.cursor()
    cursor.execute(
        "SELECT mail_id, sender_display_name, subject, is_read FROM mailbox WHERE recipient_user_id = ? ORDER BY is_read ASC, timestamp_sent_ts DESC LIMIT 20",
        (user_id,))
    mails = cursor.fetchall();
    conn.close()
    text = "📬 Ваши письма:" if mails else "📭 Ваша почта пуста."
    try:
        await query.edit_message_text(text=text, reply_markup=get_mailbox_keyboard(mails, user_id))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Почта {user_id} не изм.")
        else:
            raise


async def read_mail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    mail_id_to_read = query.data.split('_', 2)[2]
    conn = get_db_connection();
    cursor = conn.cursor()
    cursor.execute(
        "SELECT sender_display_name, subject, body, timestamp_sent_ts FROM mailbox WHERE mail_id = ? AND recipient_user_id = ?",
        (mail_id_to_read, user_id))
    mail = cursor.fetchone()
    if not mail: await query.edit_message_text("Письмо не найдено.", reply_markup=get_read_mail_keyboard(
        mail_id_to_read)); conn.close(); return
    cursor.execute("UPDATE mailbox SET is_read = 1 WHERE mail_id = ?", (mail_id_to_read,));
    conn.commit();
    conn.close()
    sent_time = datetime.fromtimestamp(mail['timestamp_sent_ts'], tz=timezone.utc).strftime('%d.%m.%Y %H:%M %Z')
    mail_text = (
        f"<b>От:</b> {mail['sender_display_name']}\n<b>Тема:</b> {mail['subject']}\n<b>Дата:</b> {sent_time}\n\n{mail['body']}")
    try:
        await query.edit_message_text(text=mail_text, reply_markup=get_read_mail_keyboard(mail_id_to_read),
                                      parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Письмо {mail_id_to_read} не изм.")
        else:
            raise


async def support_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    user_is_admin = user_id in ADMIN_USER_IDS
    text = "📞 Поддержка\n\nЗдесь вы можете обратиться за помощью, обжаловать штраф или подать на конкретного игрока в суд."
    if user_is_admin: text = "📞 Меню Поддержки (Администратор)"
    try:
        await query.edit_message_text(text=text, reply_markup=get_support_menu_keyboard(user_is_admin))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Меню поддержки {user_id} не изм.")
        else:
            raise


async def create_ticket_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if await check_maintenance(update, context): return ConversationHandler.END
    query = update.callback_query
    if query: await query.answer()
    text_to_send = "📝 Опишите вашу проблему/вопрос одним сообщением:"
    if query and query.message:
        context.user_data['support_message_id_to_edit'] = query.message.message_id
        context.user_data['support_chat_id_to_edit'] = query.message.chat_id
        await query.edit_message_text(text_to_send)
    elif update.message:
        await update.message.reply_text(text_to_send)
    return ASK_TICKET_MESSAGE


async def ask_ticket_message_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text: await update.message.reply_text(
        "Отправьте текст."); return ASK_TICKET_MESSAGE
    context.user_data['ticket_text_draft'] = update.message.text
    chat_id_to_edit = context.user_data.get('support_chat_id_to_edit')
    message_id_to_edit = context.user_data.get('support_message_id_to_edit')
    preview_text = (f"Отправить обращение:\n\n<i>{context.user_data['ticket_text_draft']}</i>\n\nПодтвердите.")
    if chat_id_to_edit and message_id_to_edit:
        try:
            await context.bot.edit_message_text(chat_id=chat_id_to_edit, message_id=message_id_to_edit,
                                                text=preview_text, reply_markup=get_confirm_ticket_keyboard(),
                                                parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"Не ред. для подтв. тикета: {e}"); await update.message.reply_text(preview_text,
                                                                                               reply_markup=get_confirm_ticket_keyboard(),
                                                                                               parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(preview_text, reply_markup=get_confirm_ticket_keyboard(),
                                        parse_mode=ParseMode.HTML)
    return CONFIRM_TICKET_SEND


async def send_confirmed_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query;
    await query.answer();
    user = query.from_user
    ticket_text = context.user_data.get('ticket_text_draft')
    is_admin_check_on_fail = query.from_user.id in ADMIN_USER_IDS  # Для reply_markup в случае ошибки
    if not ticket_text: await query.edit_message_text("Ошибка: текст обращения не найден.",
                                                      reply_markup=get_support_menu_keyboard(
                                                          is_admin_check_on_fail)); return ConversationHandler.END

    conn = get_db_connection();
    cursor = conn.cursor()
    ticket_id = generate_short_id()
    while True:
        cursor.execute("SELECT ticket_id FROM support_tickets WHERE ticket_id = ?", (ticket_id,))
        if not cursor.fetchone(): break
        ticket_id = generate_short_id()

    timestamp_created_ts = datetime.now(timezone.utc).timestamp()
    try:
        cursor.execute(
            "INSERT INTO support_tickets (ticket_id, user_id, user_message, timestamp_created_ts, status, assigned_admin_id, admin_reply, timestamp_admin_replied_ts) VALUES (?, ?, ?, ?, 'open', NULL, NULL, NULL)",
            (ticket_id, user.id, ticket_text, timestamp_created_ts));
        conn.commit()
        logger.info(f"User {user.id} создал тикет {ticket_id}.")
        success_message_part1 = escape_markdown_v2("✅ Ваше обращение принято.\nНомер: ")
        final_success_message = success_message_part1 + f"`{escape_markdown_v2(ticket_id)}`"
        await query.edit_message_text(text=final_success_message, parse_mode=ParseMode.MARKDOWN_V2,
                                      reply_markup=get_support_menu_keyboard(user.id in ADMIN_USER_IDS))
        user_display_name = get_user_display_name_from_db(user.id)
        admin_notification_text = (
            f"🔔 Новое обращение от {user_display_name} (ID: {user.id})\nТикет ID: `{escape_markdown_v2(ticket_id)}`\n\n<b>Текст:</b>\n<i>{ticket_text}</i>")
        admin_ticket_keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("✅ Принять", callback_data=f"take_ticket_{ticket_id}")]])
        for admin_id_to_notify in ADMIN_USER_IDS:
            try:
                await context.bot.send_message(chat_id=admin_id_to_notify, text=admin_notification_text,
                                               reply_markup=admin_ticket_keyboard, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Не уведомить админа {admin_id_to_notify} о тикете: {e}")
    except sqlite3.Error as e:
        logger.error(f"БД сохр. тикета: {e}")
        await query.edit_message_text("❌ Ошибка сохр.", reply_markup=get_support_menu_keyboard(is_admin_check_on_fail))
    finally:
        conn.close()
        for key_to_del in ['ticket_text_draft', 'support_message_id_to_edit', 'support_chat_id_to_edit']:
            if key_to_del in context.user_data: del context.user_data[key_to_del]
    return ConversationHandler.END


async def cancel_ticket_creation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query;
    await query.answer()
    await query.edit_message_text("Создание обращения отменено.",
                                  reply_markup=get_support_menu_keyboard(query.from_user.id in ADMIN_USER_IDS))
    for key in ['ticket_text_draft', 'support_message_id_to_edit', 'support_chat_id_to_edit']:
        if key in context.user_data: del context.user_data[key]
    return ConversationHandler.END


async def take_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    admin_user = query.from_user
    if admin_user.id not in ADMIN_USER_IDS: logger.warning(f"{admin_user.id} не админ пытался взять тикет."); return
    ticket_id_to_take = query.data.split('_', 2)[2];
    admin_display_name = get_user_display_name_from_db(admin_user.id)
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT status, assigned_admin_id, user_id, user_message FROM support_tickets WHERE ticket_id = ?",
            (ticket_id_to_take,))
        ticket = cursor.fetchone()
        current_message_text_html = query.message.text_html if query.message.text_html else escape_markdown_v2(
            query.message.text)
        if not ticket: new_text = current_message_text_html + f"\n\n❌ Тикет {ticket_id_to_take} не найден."; await query.edit_message_text(
            text=new_text, parse_mode=ParseMode.HTML); return
        if ticket['status'] != 'open': current_assignee_id = ticket[
            'assigned_admin_id']; current_assignee_name = get_user_display_name_from_db(
            current_assignee_id) if current_assignee_id else "другим"; new_text = current_message_text_html + f"\n\nℹ️ Тикет {ticket_id_to_take} уже взят {current_assignee_name}."; await query.edit_message_text(
            text=new_text, parse_mode=ParseMode.HTML, reply_markup=None); return
        cursor.execute(
            "UPDATE support_tickets SET status = 'pending_admin_reply', assigned_admin_id = ? WHERE ticket_id = ?",
            (admin_user.id, ticket_id_to_take));
        conn.commit()
        logger.info(f"{admin_display_name} ({admin_user.id}) принял тикет {ticket_id_to_take}.")
        edited_admin_notification_text = (
                    current_message_text_html + f"\n\n➡️ <b>Принят: {admin_display_name}</b>\nДля ответа: <code>/replyticket {ticket_id_to_take} ТЕКСТ</code>")
        await query.edit_message_text(text=edited_admin_notification_text, parse_mode=ParseMode.HTML, reply_markup=None)
        user_message_escaped_for_md = escape_markdown_v2(ticket['user_message']);
        ticket_id_escaped = escape_markdown_v2(ticket_id_to_take)
        user_who_sent_ticket_display_name_escaped = escape_markdown_v2(get_user_display_name_from_db(ticket['user_id']))
        admin_guidance_text = (
            f"Вы приняли обращение \\#{ticket_id_escaped}\\.\nПользователь: {user_who_sent_ticket_display_name_escaped}\nСообщение: _{user_message_escaped_for_md}_\n\nДля ответа:\n`/replyticket {ticket_id_escaped} ВАШ ОТВЕТ`")
        await context.bot.send_message(chat_id=admin_user.id, text=admin_guidance_text,
                                       parse_mode=ParseMode.MARKDOWN_V2)
    except TelegramError as te:
        if "Message is not modified" in str(te):
            logger.info(f"Увед. о тикете {ticket_id_to_take} не изм.")
        elif "message to edit not found" in str(te).lower():
            await query.message.reply_text(f"Тикет {ticket_id_to_take} уже обработан.")
        else:
            logger.error(f"TG ошибка взят. тикета {ticket_id_to_take}: {te}")
    except sqlite3.Error as e:
        logger.error(f"БД взят. тикета {ticket_id_to_take} админом {admin_user.id}: {e}");
    finally:
        conn.close()


async def main_menu_nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    try:
        await query.edit_message_text(text="👋 Главное меню:", reply_markup=get_main_menu_keyboard(user_id))
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Глав. меню {user_id} не изм.")
        else:
            raise


async def my_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT ticket_id, user_message, status, admin_reply, timestamp_created_ts, timestamp_admin_replied_ts FROM support_tickets WHERE user_id = ? ORDER BY timestamp_created_ts DESC LIMIT 10",
            (user_id,))
        tickets = cursor.fetchall()
        text = "📑 Ваши обращения:\n\n" if tickets else "У вас нет обращений."
        if tickets:
            for ticket in tickets:
                created_time = datetime.fromtimestamp(ticket['timestamp_created_ts'], tz=timezone.utc).strftime(
                    '%d.%m.%y %H:%M')
                status_map = {'open': 'Открыт', 'pending_admin_reply': 'В работе', 'admin_replied': 'Отвечен',
                              'closed': 'Закрыт'}
                status_text = status_map.get(ticket['status'], ticket['status'])
                user_msg_preview = ticket['user_message'][:50] + ("..." if len(ticket['user_message']) > 50 else "")
                text += f"<b>ID:</b> <code>{ticket['ticket_id']}</code> ({created_time})\n<b>Статус:</b> {status_text}\n<b>Сообщение:</b> <i>{user_msg_preview}</i>\n"
                if ticket['admin_reply']: replied_time_ts = ticket[
                    'timestamp_admin_replied_ts']; replied_time_str = datetime.fromtimestamp(replied_time_ts,
                                                                                             tz=timezone.utc).strftime(
                    '%d.%m.%y %H:%M') if replied_time_ts else "N/A"; text += f"<b>Ответ ({replied_time_str}):</b> <i>{ticket['admin_reply']}</i>\n"
                text += "--------------------\n"
        try:
            await query.edit_message_text(text=text, reply_markup=get_support_menu_keyboard(user_id in ADMIN_USER_IDS),
                                          parse_mode=ParseMode.HTML)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                logger.info(f"'Мои обращения' {user_id} не изм.")
            else:
                raise
    except sqlite3.Error as e:
        logger.error(f"БД тикетов {user_id}: {e}"); await query.edit_message_text("Ошибка загрузки.",
                                                                                  reply_markup=get_support_menu_keyboard(
                                                                                      user_id in ADMIN_USER_IDS));
    finally:
        conn.close()


async def view_all_tickets_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    admin_id = query.from_user.id
    if admin_id not in ADMIN_USER_IDS: logger.warning(f"{admin_id} не админ пытался смотр. все тикеты."); return
    conn = get_db_connection();
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT ticket_id, user_id, user_message, status, assigned_admin_id, timestamp_created_ts FROM support_tickets ORDER BY CASE status WHEN 'open' THEN 1 WHEN 'pending_admin_reply' THEN 2 WHEN 'admin_replied' THEN 3 ELSE 4 END, timestamp_created_ts DESC LIMIT 20")
        tickets = cursor.fetchall()
        text = "🎫 Все обращения в поддержку:\n\n" if tickets else "Нет активных обращений."
        if tickets:
            for ticket in tickets:
                created_time = datetime.fromtimestamp(ticket['timestamp_created_ts'], tz=timezone.utc).strftime(
                    '%d.%m.%y %H:%M')
                status_map = {'open': '❕Открыт', 'pending_admin_reply': '⏳В работе', 'admin_replied': '✅Отвечен',
                              'closed': '🔒Закрыт'}
                status_text = status_map.get(ticket['status'], ticket['status'])
                user_msg_preview = ticket['user_message'][:40] + ("..." if len(ticket['user_message']) > 40 else "")
                creator_display = get_user_display_name_from_db(ticket['user_id'])
                assignee_display = f"(взят {get_user_display_name_from_db(ticket['assigned_admin_id'])})" if ticket[
                    'assigned_admin_id'] else ""
                text += f"<b>ID:</b> <code>{ticket['ticket_id']}</code> ({created_time}) от {creator_display}\n<b>Статус:</b> {status_text} {assignee_display}\n<b>Сообщение:</b> <i>{user_msg_preview}</i>\n"
                # Кнопку "Взять в работу" лучше отправлять отдельным сообщением при создании тикета админам, здесь она неинтерактивна.
                # if ticket['status'] == 'open': text += f"└─ Взять: /take_ticket_cmd {ticket['ticket_id']}\n" # Пример, если бы была команда
                if ticket['status'] == 'pending_admin_reply' and ticket[
                    'assigned_admin_id'] == admin_id: text += f"└─ Ответить: <code>/replyticket {ticket['ticket_id']} текст</code>\n"
                text += "--------------------\n"
        try:
            await query.edit_message_text(text=text, reply_markup=get_support_menu_keyboard(True),
                                          parse_mode=ParseMode.HTML)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                logger.info(f"Все тикеты админ {admin_id} не изм.")
            else:
                raise
    except sqlite3.Error as e:
        logger.error(f"БД все тикеты админ {admin_id}: {e}"); await query.edit_message_text("Ошибка загрузки.",
                                                                                            reply_markup=get_support_menu_keyboard(
                                                                                                True));
    finally:
        conn.close()


async def show_reputation_placeholder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    ensure_user_in_db(user_id, query.from_user.username)
    user_display_name = get_user_display_name_from_db(user_id);
    conn = get_db_connection();
    cursor = conn.cursor()
    cursor.execute("SELECT reputation FROM users WHERE user_id = ?", (user_id,));
    user_row = cursor.fetchone()
    rep_score = user_row["reputation"] if user_row else 0;
    conn.close()
    logger.info(f"{user_display_name} реп: {rep_score}")
    try:
        await query.edit_message_text(text=f"{user_display_name}, ваша реп: {rep_score} ✨",
                                      reply_markup=get_reputation_view_keyboard())
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Реп {user_id} не изм.")
        else:
            raise


async def show_fines_placeholder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_maintenance(update, context): return
    query = update.callback_query;
    await query.answer();
    user_id = query.from_user.id
    ensure_user_in_db(user_id, query.from_user.username)
    user_display_name = get_user_display_name_from_db(user_id);
    conn = get_db_connection();
    cursor = conn.cursor()
    logger.info(f"{user_display_name} штрафы.")
    cursor.execute(
        "SELECT amount, due_date_ts, payment_code FROM fines WHERE user_id = ? AND is_paid = 0 ORDER BY due_date_ts ASC",
        (user_id,))
    active_fines_rows = cursor.fetchall();
    conn.close()
    if not active_fines_rows:
        message_text = f"{user_display_name}, нет акт. штрафов. 🎉"
    else:
        message_text = f"{user_display_name}, ваши штрафы:\n\n"
        for fine_item_row in active_fines_rows:
            due_date_fine = datetime.fromtimestamp(fine_item_row['due_date_ts'], tz=timezone.utc)
            due_date_str_fine = due_date_fine.strftime('%d.%m.%Y %H:%M %Z')
            is_overdue = datetime.now(timezone.utc) > due_date_fine;
            overdue_text = " (ПРОСРОЧЕН!)" if is_overdue else ""
            message_text += (
                f"🔢 Сумма: {fine_item_row['amount']}\n🗓️ Срок: {due_date_str_fine}{overdue_text}\n<i>Код: {fine_item_row['payment_code']}</i>\n\n")
        message_text = message_text.strip()
    try:
        await query.edit_message_text(text=message_text, reply_markup=get_fines_view_keyboard(),
                                      parse_mode=ParseMode.HTML)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.info(f"Штрафы {user_id} не изм.")
        else:
            raise


async def error_handler_callback(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error {context.error}", exc_info=context.error)
    if isinstance(update, Update) and update.effective_chat:
        try:
            if isinstance(context.error, BadRequest) and "Message is not modified" in str(context.error): logger.info(
                f"Ignored BadRequest (Msg not modified): {context.error}"); return
            if isinstance(context.error, TelegramError) and (
                    "message to edit not found" in str(context.error).lower() or "message can't be edited" in str(
                context.error).lower() or "query is too old" in str(context.error).lower()): logger.warning(
                f"Handled TelegramError: {context.error}"); return
            # await context.bot.send_message(chat_id=update.effective_chat.id, text="Внутр. ошибка. Попробуйте позже.")
        except Exception as e:
            logger.error(f"Не отпр. сообщ. об ошибке: {e}")


def main() -> None:
    if TOKEN == "YOUR_ACTUAL_BOT_TOKEN": print("!!! ЗАМЕНИТЕ ТОКЕН !!!"); return
    # if ADMIN_USER_IDS == [...]: print("!!! ПРОВЕРЬТЕ ADMIN_USER_IDS !!!")

    init_db()
    persistence = PicklePersistence(filepath=PERSISTENCE_FILE)
    application = Application.builder().token(TOKEN).persistence(persistence).build()

    if MAINTENANCE_KEY not in application.bot_data:
        application.bot_data[MAINTENANCE_KEY] = False

    protected_command_handler = ConversationHandler(
        entry_points=[CommandHandler('add', add_command_password_entry),
                      CommandHandler('fine', fine_command_password_entry),
                      CommandHandler('delfine', delfine_command_password_entry),
                      CommandHandler('sendmail', sendmail_command_password_entry),
                      CommandHandler('replyticket', replyticket_command_password_entry),
                      CommandHandler('tex', tex_command_password_entry),
                      CommandHandler('texupd', texupd_command_password_entry)],
        states={WAITING_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_password_and_execute)]},
        fallbacks=[CommandHandler('cancel', cancel_password_entry)], conversation_timeout=60)
    application.add_handler(protected_command_handler)

    create_ticket_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(create_ticket_entry, pattern='^create_ticket_entry$')],
        states={ASK_TICKET_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_ticket_message_received)],
                CONFIRM_TICKET_SEND: [
                    CallbackQueryHandler(send_confirmed_ticket_callback, pattern='^send_confirmed_ticket$'),
                    CallbackQueryHandler(cancel_ticket_creation_callback, pattern='^cancel_ticket_creation$')]},
        fallbacks=[CallbackQueryHandler(cancel_ticket_creation_callback, pattern='^cancel_ticket_creation$'),
                   CommandHandler('cancel', cancel_ticket_creation_callback)],
        map_to_parent={ConversationHandler.END: ConversationHandler.END})

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(main_menu_nav_callback, pattern='^main_menu_nav$'))
    application.add_handler(CallbackQueryHandler(show_reputation_placeholder, pattern='^show_reputation$'))
    application.add_handler(CallbackQueryHandler(show_fines_placeholder, pattern='^show_fines$'))
    application.add_handler(CallbackQueryHandler(show_mailbox_callback, pattern='^show_mailbox_main$'))
    application.add_handler(CallbackQueryHandler(read_mail_callback, pattern='^read_mail_'))
    application.add_handler(CallbackQueryHandler(support_menu_callback, pattern='^support_menu$'))
    application.add_handler(CallbackQueryHandler(view_all_tickets_admin_callback, pattern='^view_all_tickets$'))
    application.add_handler(create_ticket_conv_handler)
    application.add_handler(CallbackQueryHandler(take_ticket_callback, pattern='^take_ticket_'))
    application.add_handler(CallbackQueryHandler(my_tickets_callback, pattern='^my_tickets$'))

    application.add_error_handler(error_handler_callback)

    logger.info("Бот запускается...")
    application.run_polling()
    logger.info("Бот остановлен.")


if __name__ == "__main__":
    main()
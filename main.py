# --- START OF FILE main.py ---

import logging
import asyncio
import os
import signal
import sqlite3 # Keep for error handling if needed directly
from functools import wraps
from datetime import timedelta

# --- Telegram Imports ---
from telegram import Update, BotCommand, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application, ApplicationBuilder, Defaults, ContextTypes,
    CommandHandler, CallbackQueryHandler, MessageHandler, filters,
    PicklePersistence, JobQueue
)
from telegram.constants import ParseMode
import telegram.error as telegram_error

# --- Library Imports ---
from aiocryptopay import AioCryptoPay, Networks
import pytz

# --- Local Imports ---
# Import variables/functions that were modified or needed
from utils import (
    TOKEN, ADMIN_ID, init_db, load_all_data, LANGUAGES, THEMES,
    SUPPORT_USERNAME, BASKET_TIMEOUT, clear_all_expired_baskets,
    CRYPTOPAY_API_TOKEN, SECONDARY_ADMIN_IDS,
    get_db_connection, # Import the DB connection helper
    DATABASE_PATH # Import DB path if needed for direct error checks (optional)
)
from user import (
    start, handle_shop, handle_city_selection, handle_district_selection,
    handle_type_selection, handle_product_selection, handle_add_to_basket,
    handle_view_basket, handle_clear_basket, handle_remove_from_basket,
    handle_profile, handle_language_selection, handle_price_list,
    handle_price_list_city, handle_reviews_menu, handle_leave_review,
    handle_view_reviews, handle_leave_review_message, handle_back_start,
    handle_user_discount_code_message, apply_discount_start, remove_discount,
    handle_leave_review_now, handle_refill, handle_view_history,
    handle_refill_amount_message, validate_discount_code
)
from admin import (
    handle_admin_menu, handle_sales_analytics_menu, handle_sales_dashboard,
    handle_sales_select_period, handle_sales_run, handle_adm_city, handle_adm_dist,
    handle_adm_type, handle_adm_add, handle_adm_size, handle_adm_custom_size,
    handle_confirm_add_drop, cancel_add, handle_adm_manage_cities, handle_adm_add_city,
    handle_adm_edit_city, handle_adm_delete_city, handle_adm_manage_districts,
    handle_adm_manage_districts_city, handle_adm_add_district, handle_adm_edit_district,
    handle_adm_remove_district, handle_adm_manage_products, handle_adm_manage_products_city,
    handle_adm_manage_products_dist, handle_adm_manage_products_type, handle_adm_delete_prod,
    handle_adm_manage_types, handle_adm_add_type, handle_adm_delete_type,
    handle_adm_manage_discounts, handle_adm_toggle_discount, handle_adm_delete_discount,
    handle_adm_add_discount_start, handle_adm_use_generated_code, handle_adm_set_discount_type,
    handle_adm_set_media,
    handle_adm_broadcast_start, handle_cancel_broadcast,
    handle_confirm_broadcast, handle_adm_broadcast_message,
    handle_confirm_yes,
    handle_adm_add_city_message,
    handle_adm_add_district_message, handle_adm_edit_district_message,
    handle_adm_edit_city_message, handle_adm_custom_size_message, handle_adm_price_message,
    handle_adm_drop_details_message, handle_adm_bot_media_message, handle_adm_add_type_message,
    process_discount_code_input, handle_adm_discount_code_message, handle_adm_discount_value_message,
    handle_adm_manage_reviews, handle_adm_delete_review_confirm
)
from viewer_admin import (
    handle_viewer_admin_menu,
    handle_viewer_added_products,
    handle_viewer_view_product_media
)
from payment import (
    handle_confirm_pay, handle_check_cryptobot_payment, close_cryptopay_client,
    handle_select_refill_crypto
)
from stock import handle_view_stock

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Callback Data Parsing Decorator ---
def callback_query_router(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if query and query.data:
            parts = query.data.split('|')
            command = parts[0]
            params = parts[1:]
            target_func_name = f"handle_{command}"

            # Map command strings to the actual function objects
            KNOWN_HANDLERS = {
                # User Handlers
                "start": start, "back_start": handle_back_start, "shop": handle_shop,
                "city": handle_city_selection, "dist": handle_district_selection,
                "type": handle_type_selection, "product": handle_product_selection,
                "add": handle_add_to_basket, "view_basket": handle_view_basket,
                "clear_basket": handle_clear_basket, "remove": handle_remove_from_basket,
                "profile": handle_profile, "language": handle_language_selection,
                "price_list": handle_price_list, "price_list_city": handle_price_list_city,
                "reviews": handle_reviews_menu, "leave_review": handle_leave_review,
                "view_reviews": handle_view_reviews, "leave_review_now": handle_leave_review_now,
                "refill": handle_refill,
                "view_history": handle_view_history,
                "apply_discount_start": apply_discount_start, "remove_discount": remove_discount,
                # Payment Handlers
                "confirm_pay": handle_confirm_pay, "check_crypto_payment": handle_check_cryptobot_payment,
                "select_refill_crypto": handle_select_refill_crypto,
                # Primary Admin Handlers
                "admin_menu": handle_admin_menu,
                "sales_analytics_menu": handle_sales_analytics_menu, "sales_dashboard": handle_sales_dashboard,
                "sales_select_period": handle_sales_select_period, "sales_run": handle_sales_run,
                "adm_city": handle_adm_city, "adm_dist": handle_adm_dist, "adm_type": handle_adm_type,
                "adm_add": handle_adm_add, "adm_size": handle_adm_size, "adm_custom_size": handle_adm_custom_size,
                "confirm_add_drop": handle_confirm_add_drop, "cancel_add": cancel_add,
                "adm_manage_cities": handle_adm_manage_cities, "adm_add_city": handle_adm_add_city,
                "adm_edit_city": handle_adm_edit_city, "adm_delete_city": handle_adm_delete_city,
                "adm_manage_districts": handle_adm_manage_districts, "adm_manage_districts_city": handle_adm_manage_districts_city,
                "adm_add_district": handle_adm_add_district, "adm_edit_district": handle_adm_edit_district,
                "adm_remove_district": handle_adm_remove_district,
                "adm_manage_products": handle_adm_manage_products, "adm_manage_products_city": handle_adm_manage_products_city,
                "adm_manage_products_dist": handle_adm_manage_products_dist, "adm_manage_products_type": handle_adm_manage_products_type,
                "adm_delete_prod": handle_adm_delete_prod,
                "adm_manage_types": handle_adm_manage_types, "adm_add_type": handle_adm_add_type,
                "adm_delete_type": handle_adm_delete_type,
                "adm_manage_discounts": handle_adm_manage_discounts, "adm_toggle_discount": handle_adm_toggle_discount,
                "adm_delete_discount": handle_adm_delete_discount, "adm_add_discount_start": handle_adm_add_discount_start,
                "adm_use_generated_code": handle_adm_use_generated_code, "adm_set_discount_type": handle_adm_set_discount_type,
                "adm_set_media": handle_adm_set_media,
                "confirm_yes": handle_confirm_yes,
                "adm_broadcast_start": handle_adm_broadcast_start, "cancel_broadcast": handle_cancel_broadcast,
                "confirm_broadcast": handle_confirm_broadcast,
                "adm_manage_reviews": handle_adm_manage_reviews,
                "adm_delete_review_confirm": handle_adm_delete_review_confirm,
                # Stock Handler
                "view_stock": handle_view_stock,
                # Viewer Admin Handlers
                "viewer_admin_menu": handle_viewer_admin_menu,
                "viewer_added_products": handle_viewer_added_products,
                "viewer_view_product_media": handle_viewer_view_product_media
            }

            target_func = KNOWN_HANDLERS.get(command)

            if target_func and asyncio.iscoroutinefunction(target_func):
                await target_func(update, context, params)
            else:
                logger.warning(f"No async handler function found or mapped for callback command: {command}")
                try: await query.answer("Unknown action.", show_alert=True)
                except Exception as e: logger.error(f"Error answering unknown callback query {command}: {e}")
        elif query:
            logger.warning("Callback query handler received update without data.")
            try: await query.answer()
            except Exception as e: logger.error(f"Error answering callback query without data: {e}")
        else:
            logger.warning("Callback query handler received update without query object.")
    return wrapper

@callback_query_router
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This function is now primarily a dispatcher via the decorator.
    pass # Decorator handles everything

# --- Central Message Handler (for states) ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles regular messages based on user state."""
    if not update.message or not update.effective_user: return

    user_id = update.effective_user.id
    state = context.user_data.get('state')
    logger.debug(f"Message received from user {user_id}, state: {state}")

    STATE_HANDLERS = {
        'awaiting_review': handle_leave_review_message,
        'awaiting_user_discount_code': handle_user_discount_code_message,
        # Admin Message Handlers
        'awaiting_new_city_name': handle_adm_add_city_message,
        'awaiting_edit_city_name': handle_adm_edit_city_message,
        'awaiting_new_district_name': handle_adm_add_district_message,
        'awaiting_edit_district_name': handle_adm_edit_district_message,
        'awaiting_new_type_name': handle_adm_add_type_message,
        'awaiting_custom_size': handle_adm_custom_size_message,
        'awaiting_price': handle_adm_price_message,
        'awaiting_drop_details': handle_adm_drop_details_message,
        'awaiting_bot_media': handle_adm_bot_media_message,
        'awaiting_broadcast_message': handle_adm_broadcast_message,
        'awaiting_discount_code': handle_adm_discount_code_message,
        'awaiting_discount_value': handle_adm_discount_value_message,
        'awaiting_refill_amount': handle_refill_amount_message,
        'awaiting_refill_crypto_choice': None, # State handled by callback
    }

    handler_func = STATE_HANDLERS.get(state)
    if handler_func:
        await handler_func(update, context)
    else:
        logger.debug(f"Ignoring message from user {user_id} in state: {state}")

# --- Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs errors caused by Updates."""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    chat_id = None
    if isinstance(update, Update) and update.effective_chat:
        chat_id = update.effective_chat.id
    if chat_id:
        error_message = "An internal error occurred. Please try again later or contact support."
        if isinstance(context.error, telegram_error.BadRequest):
            logger.warning(f"Telegram API BadRequest: {context.error}")
            error_message = "An error occurred communicating with Telegram. Please try again."
        elif isinstance(context.error, telegram_error.NetworkError):
            logger.warning(f"Telegram API NetworkError: {context.error}")
            error_message = "A network error occurred. Please check your connection and try again."
        elif isinstance(context.error, sqlite3.Error):
            logger.error(f"Database error during update handling: {context.error}", exc_info=True)
            # Don't expose detailed DB errors to the user
        else:
             logger.exception("An unexpected error occurred during update handling.")
             error_message = "An unexpected error occurred. Please contact support."
        try:
            await context.bot.send_message(chat_id=chat_id, text=error_message, parse_mode=None)
        except Exception as e:
            logger.error(f"Failed to send error message to user {chat_id}: {e}")

# --- Bot Setup Functions ---
async def post_init(application: Application) -> None:
    """Post-initialization tasks, including job scheduling."""
    logger.info("Running post_init setup...")
    # init_db() and load_all_data() are called in utils.py when it's imported now
    logger.info("Setting bot commands...")
    await application.bot.set_my_commands([
        BotCommand("start", "Start the bot / Main menu"),
        BotCommand("admin", "Access admin panel (Admin only)"),
    ])
    if BASKET_TIMEOUT > 0:
        job_queue = application.job_queue
        if job_queue:
            logger.info(f"Setting up background job for expired baskets (interval: 60s)...")
            job_queue.run_repeating(clear_expired_baskets_job, interval=timedelta(seconds=60), first=timedelta(seconds=10), name="clear_baskets")
            logger.info("Background job setup complete.")
        else: logger.warning("Job Queue is not available.")
    else: logger.warning("BASKET_TIMEOUT is not positive. Skipping background job setup.")
    logger.info("Post_init finished.")

async def post_shutdown(application: Application) -> None:
    """Tasks to run on graceful shutdown."""
    logger.info("Running post_shutdown cleanup...")
    await close_cryptopay_client()
    logger.info("Post_shutdown finished.")

# Background Job Wrapper
async def clear_expired_baskets_job(context: ContextTypes.DEFAULT_TYPE):
     """Wrapper function to call the synchronous clear_all_expired_baskets."""
     logger.debug("Running background job: clear_expired_baskets_job")
     try:
         # Run the synchronous DB operation in a thread to avoid blocking asyncio loop
         await asyncio.to_thread(clear_all_expired_baskets)
         logger.info("Background job: Cleared expired baskets.")
     except Exception as e:
          logger.error(f"Error in background job clear_expired_baskets_job: {e}", exc_info=True)

# --- Main Function ---
def main() -> None:
    """Start the bot."""
    logger.info("Starting bot...")
    # Config validation happens in utils.py now
    defaults = Defaults(parse_mode=None, block=False) # Default to plain text
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .defaults(defaults)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    # Command Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", handle_admin_menu))
    # Callback Query Handler
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    # Message Handler
    application.add_handler(MessageHandler(
        (filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL,
        handle_message
    ))
    # Error Handler
    application.add_error_handler(error_handler)
    logger.info("Starting bot polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Bot polling stopped.")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually.")
    except SystemExit as e: # Catch SystemExit from config/db errors in utils
         logger.critical(f"SystemExit called: {e}")
    except Exception as e:
         logger.critical(f"Critical error in main execution: {e}", exc_info=True)

# --- END OF FILE main.py ---
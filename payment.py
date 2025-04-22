# --- START OF FILE payment.py ---

import logging
import sqlite3
import time
import os
import shutil
import asyncio
from decimal import Decimal, ROUND_UP # Use Decimal for precision
from datetime import datetime
# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode # Keep import for reference
from telegram.ext import ContextTypes # Use ContextTypes
from telegram import helpers # Keep for potential non-escaping uses
import telegram.error as telegram_error
from telegram import InputMediaPhoto, InputMediaVideo, InputMediaAnimation # Import InputMedia types
# -------------------------
from aiocryptopay import AioCryptoPay, Networks # Import the library

# Import necessary items from utils and user
from utils import (
    CRYPTOPAY_API_TOKEN, send_message_with_retry, format_currency, ADMIN_ID,
    LANGUAGES, load_all_data, BASKET_TIMEOUT,
    get_db_connection, MEDIA_DIR, # Import helper and MEDIA_DIR
    clear_expired_basket
)
# Import user module to call functions like clear_expired_basket, validate_discount_code
import user
from collections import Counter, defaultdict # Import Counter and defaultdict

logger = logging.getLogger(__name__)

# --- Initialize CryptoPay Client ---
cryptopay = None
if CRYPTOPAY_API_TOKEN:
    try:
        cryptopay = AioCryptoPay(token=CRYPTOPAY_API_TOKEN, network=Networks.MAIN_NET)
        logger.info("AioCryptoPay client initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize AioCryptoPay client: {e}", exc_info=True)
else:
     logger.warning("CRYPTOPAY_API_TOKEN not set. Crypto payments disabled.")


# --- Helper to close the CryptoPay client ---
async def close_cryptopay_client():
    if cryptopay and hasattr(cryptopay, 'close') and asyncio.iscoroutinefunction(cryptopay.close):
        try:
            await cryptopay.close()
            logger.info("CryptoPay client closed successfully.")
        except Exception as e:
            logger.error(f"Error closing cryptopay client: {e}", exc_info=True)
    elif cryptopay:
        logger.warning("cryptopay object exists but does not have an async close method.")


# --- Refill Payment Initiation (Called after rate calculation) ---
async def initiate_refill_payment_final(update: Update, context: ContextTypes.DEFAULT_TYPE, selected_asset: str, crypto_amount: Decimal, target_eur_amount: Decimal):
    """Creates the invoice using the calculated crypto amount."""
    query = update.callback_query
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    lang = context.user_data.get("lang", "en") # Get language
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not cryptopay:
        logger.error("CryptoPay client is None in initiate_refill_payment_final.")
        await query.answer("Payment system error.", show_alert=True)
        return

    # Get translated text
    preparing_invoice_msg = lang_data.get("preparing_invoice", "⏳ Preparing your payment invoice...")
    failed_invoice_creation_msg = lang_data.get("failed_invoice_creation", "❌ Failed to create payment invoice. Please try again later or contact support.")
    back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")

    try:
        await query.edit_message_text(preparing_invoice_msg, reply_markup=None, parse_mode=None) # Use None

        payload_data = f"user_{user_id}_refill_{target_eur_amount}_{int(time.time())}"
        # Ensure crypto_amount is formatted correctly for the API (float)
        crypto_amount_float = float(crypto_amount.quantize(Decimal('0.00000001'))) # Ensure enough precision

        logger.info(f"Creating CryptoBot invoice for REFILL for user {user_id}, calculated crypto amount {crypto_amount_float} {selected_asset} (target {target_eur_amount:.2f} EUR)")

        invoice = await cryptopay.create_invoice(
            asset=selected_asset,
            amount=crypto_amount_float,
            description=f"Balance top-up (~{target_eur_amount:.2f} EUR)",
            payload=payload_data,
            expires_in=900 # 15 minutes expiration
        )

        if not invoice or not invoice.invoice_id or not invoice.bot_invoice_url:
            raise ValueError("Failed to create refill invoice or received invalid response from CryptoBot API.")

        deposit_address = getattr(invoice, 'pay_address', None)
        network = getattr(invoice, 'network', selected_asset)
        # Use the actual amount from the invoice if available, otherwise the calculated one
        actual_crypto_amount = Decimal(str(invoice.amount)) if invoice.amount is not None else crypto_amount

        logger.info(f"Refill invoice created for user {user_id}: ID {invoice.invoice_id}, URL: {invoice.bot_invoice_url}, Crypto Amount: {invoice.amount} {invoice.asset}, Deposit Addr: {deposit_address}")

        # Store details needed for checking and processing
        context.user_data['pending_payment'] = {
            'invoice_id': invoice.invoice_id,
            'pay_url': invoice.bot_invoice_url,
            'asset': invoice.asset,
            'crypto_amount': actual_crypto_amount, # Store the Decimal amount
            'fiat_total': target_eur_amount, # Store the Decimal amount
            'type': 'refill',
            'deposit_address': deposit_address,
            'network': network
        }
        context.user_data.pop('refill_eur_amount', None) # Clear the intermediate value

        await display_cryptobot_invoice(update, context, invoice)

    except Exception as e:
        logger.error(f"Error creating final CryptoBot refill invoice for user {user_id}: {e}", exc_info=True)
        context.user_data.pop('refill_eur_amount', None)
        context.user_data.pop('state', None) # Reset state
        try:
            await query.edit_message_text(
                failed_invoice_creation_msg,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_profile_button}", callback_data="profile")]]),
                parse_mode=None # Use None
            )
        except Exception as edit_e:
            logger.error(f"Failed to edit message with invoice creation error: {edit_e}")
            await send_message_with_retry(context.bot, chat_id,
                failed_invoice_creation_msg,
                parse_mode=None # Use None
            )


# --- Callback Handler for Crypto Selection during Refill ---
async def handle_select_refill_crypto(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the user selecting the crypto asset for refill, gets rates, calculates crypto amount."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en") # Get language
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not params:
        logger.warning(f"handle_select_refill_crypto called without asset parameter for user {user_id}")
        await query.answer("Error: Missing crypto choice.", show_alert=True)
        return

    selected_asset = params[0].upper()
    logger.info(f"User {user_id} selected {selected_asset} for refill.")

    refill_eur_amount = context.user_data.get('refill_eur_amount')
    if not refill_eur_amount or refill_eur_amount <= 0:
        logger.error(f"Refill amount context lost before asset selection for user {user_id}.")
        await query.edit_message_text("❌ Error: Refill amount context lost. Please start the top up again.", parse_mode=None) # Use None
        context.user_data.pop('state', None) # Reset state
        return

    # Ensure refill_eur_amount is Decimal
    refill_eur_amount_decimal = Decimal(str(refill_eur_amount))

    # Get translated texts
    calculating_msg = lang_data.get("calculating_amount", "⏳ Calculating required amount and preparing invoice...")
    error_getting_rate_msg = lang_data.get("error_getting_rate", "❌ Error: Could not get exchange rate for {asset}. Please try another currency or contact support.")
    error_preparing_payment_msg = lang_data.get("error_preparing_payment", "❌ An error occurred while preparing the payment. Please try again later.")
    back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")

    try:
        await query.edit_message_text(calculating_msg, reply_markup=None, parse_mode=None) # Use None
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
            logger.warning(f"Couldn't edit message in handle_select_refill_crypto: {e}")
        await query.answer("Calculating...")

    try:
        if not cryptopay: raise RuntimeError("CryptoPay client is not available.")

        rates = await cryptopay.get_exchange_rates()
        rate_found = False
        rate_eur_to_asset = Decimal('0')

        for rate in rates:
            if rate.source == selected_asset and rate.target == 'EUR':
                # Ensure rate is Decimal for precision
                rate_eur_to_asset = Decimal(str(rate.rate))
                rate_found = True
                logger.info(f"Found rate for {selected_asset}/EUR: {rate_eur_to_asset}")
                break

        if not rate_found or rate_eur_to_asset <= 0:
            logger.error(f"Could not find a valid exchange rate for {selected_asset}/EUR.")
            try:
                await query.message.edit_text(
                    error_getting_rate_msg.format(asset=selected_asset),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_profile_button}", callback_data="profile")]]),
                    parse_mode=None # Use None
                )
            except Exception as edit_e:
                 logger.error(f"Failed to edit message with rate error: {edit_e}")
                 await send_message_with_retry(context.bot, chat_id, error_getting_rate_msg.format(asset=selected_asset), parse_mode=None) # Use None
            context.user_data.pop('state', None) # Reset state
            return

        # Calculate crypto amount using Decimal division and rounding up
        crypto_amount_needed = (refill_eur_amount_decimal / rate_eur_to_asset).quantize(Decimal('0.00000001'), rounding=ROUND_UP)
        logger.info(f"Calculated {crypto_amount_needed} {selected_asset} needed for {refill_eur_amount_decimal} EUR.")

        # Clear state before initiating final payment step
        context.user_data.pop('state', None)

        await initiate_refill_payment_final(update, context, selected_asset, crypto_amount_needed, refill_eur_amount_decimal)

    except Exception as e:
        logger.error(f"Error during refill crypto selection/rate calculation for user {user_id}: {e}", exc_info=True)
        context.user_data.pop('refill_eur_amount', None)
        context.user_data.pop('state', None) # Reset state
        try:
            await query.message.edit_text(
                error_preparing_payment_msg,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_profile_button}", callback_data="profile")]]),
                parse_mode=None # Use None
            )
        except Exception as edit_e:
             logger.error(f"Failed to edit message with calculation error: {edit_e}")
             await send_message_with_retry(context.bot, chat_id, error_preparing_payment_msg, parse_mode=None) # Use None


# --- display_cryptobot_invoice (Edits existing message) ---
async def display_cryptobot_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE, invoice):
    """Displays the CryptoBot invoice details to the user by EDITING the message."""
    query = update.callback_query
    lang = context.user_data.get("lang", "en") # Get language
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    pending_payment = context.user_data.get('pending_payment')

    if not pending_payment or not hasattr(invoice, 'bot_invoice_url') or invoice.invoice_id != pending_payment.get('invoice_id'):
        logger.warning(f"Invoice details mismatch/missing in display_cryptobot_invoice (User: {update.effective_user.id}).")
        await query.answer("Error displaying payment details (stale?).", show_alert=True)
        # Try to send user back gracefully
        payment_type = pending_payment.get('type', 'purchase') if pending_payment else 'purchase'
        fallback_callback = "profile" if payment_type == 'refill' else "view_basket"
        back_button_text = lang_data.get("back_button", "Back")
        try:
            await query.edit_message_text("❌ Payment details seem outdated. Please go back.",
                                           reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=fallback_callback)]]),
                                           parse_mode=None)
        except Exception: pass # Ignore edit error here
        return

    payment_url = invoice.bot_invoice_url
    asset = pending_payment.get('asset', 'N/A')
    crypto_amount_to_pay = pending_payment.get('crypto_amount') # Should be Decimal
    fiat_total_display = format_currency(pending_payment['fiat_total']) # Fiat total should be Decimal
    payment_type = pending_payment.get('type', 'purchase')
    deposit_address = pending_payment.get('deposit_address')
    network = pending_payment.get('network')

    crypto_amount_display = "N/A"
    if isinstance(crypto_amount_to_pay, Decimal):
        try:
            # Format Decimal precisely, removing trailing zeros after normalization
            crypto_amount_display = '{:f}'.format(crypto_amount_to_pay.normalize())
        except Exception as format_e:
            logger.error(f"Error formatting crypto amount {crypto_amount_to_pay}: {format_e}")
            crypto_amount_display = str(crypto_amount_to_pay) # Fallback to string
    elif crypto_amount_to_pay is not None:
        crypto_amount_display = str(crypto_amount_to_pay) # Fallback if not Decimal


    # Get translated texts
    invoice_title_purchase = lang_data.get("invoice_title_purchase", "Payment Invoice Created")
    invoice_title_refill = lang_data.get("invoice_title_refill", "Top-Up Invoice Created")
    please_pay_label = lang_data.get("please_pay_label", "Please pay")
    target_value_label = lang_data.get("target_value_label", "Target Value")
    alt_send_label = lang_data.get("alt_send_label", "Alternatively, send the exact amount to this address:")
    coin_label = lang_data.get("coin_label", "Coin")
    network_label = lang_data.get("network_label", "Network")
    send_warning_template = lang_data.get("send_warning_template", "⚠️ Send only {asset} via the specified network. Ensure you send at least {amount} {asset}.")
    or_click_button_label = lang_data.get("or_click_button_label", "Or click the button below:")
    invoice_expires_note = lang_data.get("invoice_expires_note", "⚠️ This invoice expires in 15 minutes. After paying, click 'Check Payment Status'.")
    pay_now_button = lang_data.get("pay_now_button_crypto", "Pay Now via CryptoBot")
    check_status_button = lang_data.get("check_status_button", "Check Payment Status")
    cancel_button = lang_data.get("cancel_button", "Cancel")

    title = invoice_title_purchase if payment_type == 'purchase' else invoice_title_refill

    msg = (
        f"{title}\n\n"
        f"{please_pay_label}: {crypto_amount_display} {asset}\n"
        f"({target_value_label}: {fiat_total_display} EUR)\n\n"
    )
    if deposit_address:
        msg += f"{alt_send_label}\n"
        msg += f"{coin_label}: {asset}\n"
        if network:
             msg += f"{network_label}: {network} ‼️\n"
        msg += f"`{deposit_address}`\n\n" # Keep backticks for easy copy
        msg += send_warning_template.format(asset=asset, amount=crypto_amount_display) + "\n\n"

    msg += f"{or_click_button_label}\n{payment_url}\n\n" # URL should not be escaped
    msg += f"{invoice_expires_note}"

    cancel_callback = "view_basket" if payment_type == 'purchase' else "profile"

    keyboard = [
        [InlineKeyboardButton(f"➡️ {pay_now_button}", url=payment_url)],
        [InlineKeyboardButton(f"✅ {check_status_button}", callback_data=f"check_crypto_payment|{invoice.invoice_id}")],
        [InlineKeyboardButton(f"⬅️ {cancel_button}", callback_data=cancel_callback)]
    ]

    try:
        if query.message:
            await query.edit_message_text(
                msg, reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=None, # Send as plain text (except for address)
                disable_web_page_preview=True
            )
        else:
            # Fallback if original message context is lost
            await display_cryptobot_invoice_new_message(update, context, invoice)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
             logger.error(f"Error editing cryptobot invoice message: {e}")
             # If editing fails, try sending a new message as fallback
             await display_cryptobot_invoice_new_message(update, context, invoice)
        else: await query.answer() # Ignore "not modified"


# --- display_cryptobot_invoice_new_message ---
async def display_cryptobot_invoice_new_message(update: Update, context: ContextTypes.DEFAULT_TYPE, invoice):
    """Displays the CryptoBot invoice details to the user by SENDING a new message."""
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    lang = context.user_data.get("lang", "en") # Get language
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    pending_payment = context.user_data.get('pending_payment')

    if not pending_payment or not hasattr(invoice, 'bot_invoice_url') or invoice.invoice_id != pending_payment.get('invoice_id'):
        logger.warning(f"Invoice details mismatch/missing in display_cryptobot_invoice_new_message (User: {user_id}).")
        await send_message_with_retry(context.bot, chat_id, "❌ Error displaying payment details (stale?).", parse_mode=None) # Use None
        return

    payment_url = invoice.bot_invoice_url
    asset = pending_payment.get('asset', 'N/A')
    crypto_amount_to_pay = pending_payment.get('crypto_amount') # Decimal
    fiat_total_display = format_currency(pending_payment['fiat_total']) # Decimal
    payment_type = pending_payment.get('type', 'purchase')
    deposit_address = pending_payment.get('deposit_address')
    network = pending_payment.get('network')

    crypto_amount_display = "N/A"
    if isinstance(crypto_amount_to_pay, Decimal):
        try: crypto_amount_display = '{:f}'.format(crypto_amount_to_pay.normalize())
        except Exception as format_e: crypto_amount_display = str(crypto_amount_to_pay)
    elif crypto_amount_to_pay is not None: crypto_amount_display = str(crypto_amount_to_pay)

    # Get translated texts (reuse from previous function)
    invoice_title_purchase = lang_data.get("invoice_title_purchase", "Payment Invoice Created")
    invoice_title_refill = lang_data.get("invoice_title_refill", "Top-Up Invoice Created")
    please_pay_label = lang_data.get("please_pay_label", "Please pay")
    target_value_label = lang_data.get("target_value_label", "Target Value")
    alt_send_label = lang_data.get("alt_send_label", "Alternatively, send the exact amount to this address:")
    coin_label = lang_data.get("coin_label", "Coin")
    network_label = lang_data.get("network_label", "Network")
    send_warning_template = lang_data.get("send_warning_template", "⚠️ Send only {asset} via the specified network. Ensure you send at least {amount} {asset}.")
    or_click_button_label = lang_data.get("or_click_button_label", "Or click the button below:")
    invoice_expires_note = lang_data.get("invoice_expires_note", "⚠️ This invoice expires in 15 minutes. After paying, click 'Check Payment Status'.")
    pay_now_button = lang_data.get("pay_now_button_crypto", "Pay Now via CryptoBot")
    check_status_button = lang_data.get("check_status_button", "Check Payment Status")
    cancel_button = lang_data.get("cancel_button", "Cancel")

    title = invoice_title_purchase if payment_type == 'purchase' else invoice_title_refill

    msg = (
        f"{title}\n\n"
        f"{please_pay_label}: {crypto_amount_display} {asset}\n"
        f"({target_value_label}: {fiat_total_display} EUR)\n\n"
    )
    if deposit_address:
        msg += f"{alt_send_label}\n"
        msg += f"{coin_label}: {asset}\n"
        if network:
             msg += f"{network_label}: {network} ‼️\n"
        msg += f"`{deposit_address}`\n\n" # Keep backticks for easy copy
        msg += send_warning_template.format(asset=asset, amount=crypto_amount_display) + "\n\n"

    msg += f"{or_click_button_label}\n{payment_url}\n\n" # URL not escaped
    msg += f"{invoice_expires_note}"

    cancel_callback = "view_basket" if payment_type == 'purchase' else "profile"

    keyboard = [
        [InlineKeyboardButton(f"➡️ {pay_now_button}", url=payment_url)],
        [InlineKeyboardButton(f"✅ {check_status_button}", callback_data=f"check_crypto_payment|{invoice.invoice_id}")],
        [InlineKeyboardButton(f"⬅️ {cancel_button}", callback_data=cancel_callback)]
    ]

    await send_message_with_retry(
        context.bot, chat_id, msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=None, # Send as plain text (except address)
        disable_web_page_preview=True
    )

# --- Payment Confirmation Check ---
async def handle_check_cryptobot_payment(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Check Payment Status' button press."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en") # Get language
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not cryptopay:
        await query.answer("Payment system is currently unavailable.", show_alert=True)
        return

    if not params:
        await query.answer("Error: Invoice ID missing.", show_alert=True)
        return

    try: invoice_id_to_check = int(params[0])
    except (ValueError, IndexError):
        await query.answer("Error: Invalid Invoice ID.", show_alert=True)
        return

    pending_payment = context.user_data.get('pending_payment')
    has_pending_payment = pending_payment is not None
    is_current_payment = has_pending_payment and pending_payment.get('invoice_id') == invoice_id_to_check

    # Get translated answer texts
    checking_previous_answer = lang_data.get("checking_previous_answer", "Checking status of previous invoice...")
    checking_cancelled_answer = lang_data.get("checking_cancelled_answer", "Checking status of cancelled invoice...")
    checking_status_answer = lang_data.get("checking_status_answer", "Checking payment status...")
    could_not_retrieve_status_msg = lang_data.get("could_not_retrieve_status", "❌ Could not retrieve invoice status.")
    error_processing_invalid_amount = lang_data.get("error_processing_invalid_amount", "❌ Error processing payment (invalid amount).")
    error_updating_balance = lang_data.get("error_updating_balance", "✅ Payment received, error updating balance. Contact support!")
    payment_confirm_order_processed = lang_data.get("payment_confirm_order_processed", "✅ Payment confirmed! Order processed.")
    unknown_payment_type_error = lang_data.get("unknown_payment_type_error", "❌ Internal error: Unknown payment type.")
    payment_received_previous_invoice = lang_data.get("payment_received_previous_invoice", "✅ Payment for previous invoice received.")
    invoice_expired_msg = lang_data.get("invoice_expired", "⏳ Invoice has expired. Please try again.")
    payment_not_detected_msg = lang_data.get("payment_not_detected", "⏳ Payment not detected yet. Check again later.")
    invoice_status_other_template = lang_data.get("invoice_status_other", "Invoice status: {status}.")
    error_checking_status_msg = lang_data.get("error_checking_status", "❌ Error checking payment status.")
    error_checking_status_api_token_msg = lang_data.get("error_checking_status_api_token", "❌ Error checking payment status (API Token). Contact support.")

    if not has_pending_payment: await query.answer(checking_previous_answer, show_alert=False)
    elif not is_current_payment: await query.answer(checking_cancelled_answer, show_alert=False)
    else: await query.answer(checking_status_answer)

    try:
        if not cryptopay: raise RuntimeError("CryptoPay client is not available.")
        invoices = await cryptopay.get_invoices(invoice_ids=[invoice_id_to_check])

        if not invoices or not invoices[0]:
            logger.warning(f"Could not find invoice {invoice_id_to_check} via CryptoBot API.")
            await send_message_with_retry(context.bot, chat_id, could_not_retrieve_status_msg, parse_mode=None)
            return

        invoice = invoices[0]
        status = invoice.status.lower()
        logger.info(f"Checked status for invoice {invoice_id_to_check}: {status}")

        if status == 'paid':
            if is_current_payment:
                payment_type = pending_payment.get('type', 'purchase')

                if payment_type == 'refill':
                    logger.info(f"Refill payment confirmed for invoice {invoice_id_to_check}.")
                    # Use fiat amount from invoice if available, otherwise use stored target
                    fiat_paid_raw = getattr(invoice, 'paid_fiat_amount', None)
                    amount_to_add = Decimal(str(fiat_paid_raw)) if fiat_paid_raw is not None else pending_payment.get('fiat_total')

                    if not isinstance(amount_to_add, Decimal) or amount_to_add <= 0:
                        logger.error(f"CRITICAL: Invalid paid amount for refill invoice {invoice_id_to_check}. Amount: {amount_to_add}")
                        try: await query.edit_message_text(error_processing_invalid_amount, parse_mode=None)
                        except: await send_message_with_retry(context.bot, chat_id, error_processing_invalid_amount, parse_mode=None)
                        context.user_data.pop('pending_payment', None)
                        return

                    # Pass Decimal amount to processing function
                    refill_success = await process_successful_refill(user_id, amount_to_add, invoice_id_to_check, context)
                    if refill_success:
                        context.user_data.pop('pending_payment', None)
                        try: await query.edit_message_text("✅ Top Up Successful! Details sent above.", parse_mode=None)
                        except telegram_error.BadRequest: logger.warning(f"Could not edit refill confirmation msg.")
                    else:
                        await send_message_with_retry(context.bot, chat_id, error_updating_balance, parse_mode=None)
                        logger.critical(f"CRITICAL: Refill paid, balance update failed for user {user_id}, invoice {invoice_id_to_check}. MANUAL ACTION REQUIRED.")

                elif payment_type == 'purchase':
                    # This flow is not active, but kept for reference
                    logger.warning(f"Purchase payment confirmed for invoice {invoice_id_to_check} (unexpected flow).")
                    purchase_success = await process_successful_cryptobot_purchase(user_id, pending_payment, context)
                    if purchase_success:
                        context.user_data.pop('pending_payment', None); context.user_data.pop('applied_discount', None); context.user_data['basket'] = []
                        try: await query.edit_message_text(payment_confirm_order_processed, parse_mode=None)
                        except telegram_error.BadRequest: logger.warning(f"Could not edit purchase confirmation msg.")
                    else: logger.critical(f"CRITICAL: Purchase paid, processing failed user {user_id}, invoice {invoice_id_to_check}. MANUAL ACTION REQUIRED.")
                else:
                     logger.error(f"Unknown payment type '{payment_type}' invoice {invoice_id_to_check}.")
                     await query.edit_message_text(unknown_payment_type_error, parse_mode=None)
                     context.user_data.pop('pending_payment', None)

            else: # Payment received for an old/non-pending invoice
                logger.warning(f"Payment received for non-pending invoice {invoice_id_to_check} by user {user_id}.")
                await send_message_with_retry(context.bot, chat_id, payment_received_previous_invoice, parse_mode=None)

        elif status == 'expired':
            await send_message_with_retry(context.bot, chat_id, invoice_expired_msg, parse_mode=None)
            if is_current_payment: context.user_data.pop('pending_payment', None)
        elif status == 'active':
            await send_message_with_retry(context.bot, chat_id, payment_not_detected_msg, parse_mode=None)
        else: # Other statuses like 'checking', 'cancelled'
             await send_message_with_retry(context.bot, chat_id, invoice_status_other_template.format(status=status), parse_mode=None)
             if is_current_payment: context.user_data.pop('pending_payment', None) # Clear pending if status is final but not 'paid'

    except Exception as e:
        logger.error(f"Error checking CryptoBot invoice status {invoice_id_to_check}: {e}", exc_info=True)
        error_text = error_checking_status_msg
        if "invalid token" in str(e).lower(): error_text = error_checking_status_api_token_msg
        await send_message_with_retry(context.bot, chat_id, error_text, parse_mode=None)


# --- process_successful_refill ---
async def process_successful_refill(user_id: int, amount_to_add: Decimal, invoice_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Handles DB updates after a CryptoBot REFILL payment is confirmed."""
    chat_id = context._chat_id or context._user_id or user_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not isinstance(amount_to_add, Decimal) or amount_to_add <= 0:
        logger.error(f"Invalid amount_to_add in process_successful_refill: {amount_to_add}")
        return False

    conn = None
    db_update_successful = False
    # Convert Decimal to float JUST for DB storage (SQLite doesn't have native Decimal)
    amount_float = float(amount_to_add)
    new_balance = 0.0 # Initialize

    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("BEGIN")
        logger.info(f"Attempting balance update for user {user_id} by {amount_float} EUR (Inv: {invoice_id})")
        update_result = c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_float, user_id))
        if update_result.rowcount == 0:
            logger.error(f"User {user_id} not found during refill DB update (Inv: {invoice_id}). Rowcount: {update_result.rowcount}")
            conn.rollback()
            return False
        # Fetch new balance
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        new_balance_result = c.fetchone()
        if new_balance_result: new_balance = new_balance_result['balance'] # Access by name
        else: logger.error(f"Could not fetch new balance for {user_id} after update."); conn.rollback(); return False
        conn.commit()
        db_update_successful = True
        logger.info(f"Successfully processed refill DB update for user {user_id}. Added: {amount_to_add} EUR. New Balance: {new_balance} EUR.")
        # Get translated texts
        top_up_success_title = lang_data.get("top_up_success_title", "✅ Top Up Successful!")
        amount_added_label = lang_data.get("amount_added_label", "Amount Added")
        new_balance_label = lang_data.get("new_balance_label", "Your new balance")
        back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")
        # Format amounts (already Decimal)
        amount_str = format_currency(amount_to_add)
        new_balance_str = format_currency(new_balance)
        success_msg = (f"{top_up_success_title}\n\n{amount_added_label}: {amount_str} EUR\n"
                       f"{new_balance_label}: {new_balance_str} EUR")
        keyboard = [[InlineKeyboardButton(f"👤 {back_to_profile_button}", callback_data="profile")]]
        await send_message_with_retry(context.bot, chat_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        return True
    except sqlite3.Error as e:
        logger.error(f"DB error during process_successful_refill user {user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        return False
    except Exception as e:
         logger.error(f"Unexpected error during process_successful_refill user {user_id}: {e}", exc_info=True)
         if conn and conn.in_transaction: conn.rollback()
         return False
    finally:
        if conn: conn.close()


# --- process_successful_cryptobot_purchase ---
async def process_successful_cryptobot_purchase(user_id, payment_details, context: ContextTypes.DEFAULT_TYPE):
    """Handles DB updates after CryptoBot payment is confirmed for PURCHASE."""
    # THIS FUNCTION IS CURRENTLY UNUSED IN THE MAIN FLOW (PAYMENT IS VIA BALANCE)
    # Kept for reference, needs similar updates as process_purchase_with_balance if activated
    logger.warning(f"process_successful_cryptobot_purchase called unexpectedly for user {user_id}")
    # Add similar logic using get_db_connection(), MEDIA_DIR, etc. if this flow is ever re-enabled.
    return False # Defaulting to False as it's unused


# --- Process Purchase with Balance ---
async def process_purchase_with_balance(user_id: int, amount_to_deduct: Decimal, basket_snapshot: list, discount_code_used: str | None, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handles DB updates when paying with internal balance."""
    chat_id = context._chat_id or context._user_id or user_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    if not basket_snapshot: logger.error(f"Empty basket_snapshot for user {user_id} balance purchase."); return False
    if not isinstance(amount_to_deduct, Decimal) or amount_to_deduct < 0: logger.error(f"Invalid amount_to_deduct {amount_to_deduct}."); return False

    conn = None
    sold_out_during_process = []
    final_pickup_details = defaultdict(list)
    db_update_successful = False
    processed_product_ids = []
    purchases_to_insert = []
    # Convert Decimal to float for DB interactions
    amount_float_to_deduct = float(amount_to_deduct)

    # Get translated texts
    balance_changed_error = lang_data.get("balance_changed_error", "❌ Transaction failed: Balance changed.")
    order_failed_all_sold_out_balance = lang_data.get("order_failed_all_sold_out_balance", "❌ Order Failed: All items sold out.")
    error_processing_purchase_contact_support = lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase. Contact support.")

    try:
        conn = get_db_connection() # Use helper
        c = conn.cursor()
        c.execute("BEGIN EXCLUSIVE")
        # 1. Verify balance
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        current_balance_result = c.fetchone()
        if not current_balance_result or current_balance_result['balance'] < amount_float_to_deduct:
             logger.warning(f"Insufficient balance user {user_id}. Needed: {amount_float_to_deduct}")
             conn.rollback()
             await send_message_with_retry(context.bot, chat_id, balance_changed_error, parse_mode=None)
             return False
        # 2. Deduct balance
        update_res = c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount_float_to_deduct, user_id))
        if update_res.rowcount == 0: logger.error(f"Failed to deduct balance for user {user_id}."); conn.rollback(); return False
        # 3. Process items
        product_ids_in_snapshot = list(set(item['product_id'] for item in basket_snapshot))
        if not product_ids_in_snapshot: logger.warning(f"Empty snapshot IDs user {user_id}."); conn.rollback(); return False
        # row_factory is already set
        placeholders = ','.join('?' * len(product_ids_in_snapshot))
        c.execute(f"SELECT id, name, product_type, size, price, city, district, available, reserved, original_text FROM products WHERE id IN ({placeholders})", product_ids_in_snapshot)
        product_db_details = {row['id']: dict(row) for row in c.fetchall()} # Access by name
        purchase_time_iso = datetime.now().isoformat()
        for item_snapshot in basket_snapshot:
            product_id = item_snapshot['product_id']
            details = product_db_details.get(product_id)
            if not details: sold_out_during_process.append(f"Item ID {product_id} (unavailable)"); continue
            # Decrement reserved first (item was held in basket)
            res_update = c.execute("UPDATE products SET reserved = MAX(0, reserved - 1) WHERE id = ?", (product_id,))
            if res_update.rowcount == 0: logger.warning(f"Failed reserve decr. P{product_id} user {user_id}."); sold_out_during_process.append(f"{details.get('name', '?')} {details.get('size', '?')}"); continue
            # Then decrement available (the actual sale)
            avail_update = c.execute("UPDATE products SET available = available - 1 WHERE id = ? AND available > 0", (product_id,)) # Ensure available > 0
            if avail_update.rowcount == 0: logger.error(f"Failed available decr. P{product_id} user {user_id}. Race?"); sold_out_during_process.append(f"{details.get('name', '?')} {details.get('size', '?')}"); c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = ?", (product_id,)); continue # Rollback reservation if available update fails
            # Add to purchase log
            item_price_from_db = float(details['price'])
            purchases_to_insert.append((user_id, product_id, details['name'], details['product_type'], details['size'], item_price_from_db, details['city'], details['district'], purchase_time_iso))
            processed_product_ids.append(product_id)
            final_pickup_details[product_id].append({'name': details['name'], 'size': details['size'], 'text': details.get('original_text')})
        if not purchases_to_insert:
            logger.warning(f"No items processed user {user_id}. Rolling back balance deduction.")
            conn.rollback()
            await send_message_with_retry(context.bot, chat_id, order_failed_all_sold_out_balance, parse_mode=None)
            return False
        # 4. Record Purchases & Update User Stats
        c.executemany("INSERT INTO purchases (user_id, product_id, product_name, product_type, product_size, price_paid, city, district, purchase_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", purchases_to_insert)
        c.execute("UPDATE users SET total_purchases = total_purchases + ? WHERE user_id = ?", (len(purchases_to_insert), user_id))
        if discount_code_used: c.execute("UPDATE discount_codes SET uses_count = uses_count + 1 WHERE code = ?", (discount_code_used,))
        c.execute("UPDATE users SET basket = '' WHERE user_id = ?", (user_id,)) # Clear DB basket
        conn.commit()
        db_update_successful = True
        logger.info(f"Processed balance purchase user {user_id}. Deducted: {amount_to_deduct} EUR.")
    except sqlite3.Error as e:
        logger.error(f"DB error during balance purchase user {user_id}: {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        db_update_successful = False
    except Exception as e:
         logger.error(f"Unexpected error during balance purchase user {user_id}: {e}", exc_info=True)
         if conn and conn.in_transaction: conn.rollback()
         db_update_successful = False
    finally:
        if conn: conn.close() # Close connection if opened

    # --- Post-Transaction Cleanup & Message Sending ---
    if db_update_successful:
        media_details = defaultdict(list)
        if processed_product_ids:
            conn_media = None # Initialize
            try:
                conn_media = get_db_connection() # Use helper
                c_media = conn_media.cursor()
                media_placeholders = ','.join('?' * len(processed_product_ids))
                # Use column names
                c_media.execute(f"SELECT product_id, media_type, telegram_file_id, file_path FROM product_media WHERE product_id IN ({media_placeholders})", processed_product_ids)
                for row in c_media.fetchall(): media_details[row['product_id']].append(dict(row))
            except sqlite3.Error as e: logger.error(f"DB error fetching media: {e}")
            finally:
                if conn_media: conn_media.close()
        # Send confirmation and details
        success_title = lang_data.get("purchase_success", "🎉 Purchase Complete! Pickup details below:")
        await send_message_with_retry(context.bot, chat_id, success_title, parse_mode=None)
        for prod_id in processed_product_ids:
            item_details = final_pickup_details.get(prod_id)
            if not item_details: continue
            item_name, item_size = item_details[0]['name'], item_details[0]['size']
            item_text = item_details[0]['text'] or "(No specific pickup details provided)"
            item_header = f"--- Item: {item_name} {item_size} ---"
            sent_media = False
            if prod_id in media_details:
                media_list = media_details[prod_id]
                if media_list:
                    media_item = media_list[0] # Send only the first media item for simplicity
                    file_id, media_type, file_path = media_item.get('telegram_file_id'), media_item.get('media_type'), media_item.get('file_path')
                    caption = item_header # Use header as caption
                    try:
                        if file_id and media_type == 'photo': await context.bot.send_photo(chat_id, photo=file_id, caption=caption, parse_mode=None); sent_media = True
                        elif file_id and media_type == 'video': await context.bot.send_video(chat_id, video=file_id, caption=caption, parse_mode=None); sent_media = True
                        elif file_id and media_type == 'gif': await context.bot.send_animation(chat_id, animation=file_id, caption=caption, parse_mode=None); sent_media = True
                        elif file_path and await asyncio.to_thread(os.path.exists, file_path): # Check existence before opening
                             # Send from file path using asyncio thread
                             async with await asyncio.to_thread(open, file_path, 'rb') as f:
                                 if media_type == 'photo': await context.bot.send_photo(chat_id, photo=f, caption=caption, parse_mode=None); sent_media = True
                                 elif media_type == 'video': await context.bot.send_video(chat_id, video=f, caption=caption, parse_mode=None); sent_media = True
                                 elif media_type == 'gif': await context.bot.send_animation(chat_id, animation=f, caption=caption, parse_mode=None); sent_media = True
                                 else: logger.warning(f"Unsupported media type '{media_type}' from path {file_path}")
                        else: logger.warning(f"Media path invalid or file missing for prod {prod_id}: {file_path}")
                    except Exception as e: logger.error(f"Error sending media P{prod_id} user {user_id}: {e}", exc_info=True)
            # Always send Text Details separately
            await send_message_with_retry(context.bot, chat_id, item_text, parse_mode=None)
            # --- Delete product record and media directory ---
            conn_del = None # Initialize
            try:
                conn_del = get_db_connection() # Use helper
                c_del = conn_del.cursor()
                # First delete media records to avoid FK issues if any remain
                c_del.execute("DELETE FROM product_media WHERE product_id = ?", (prod_id,))
                # Then delete the product record
                delete_result = c_del.execute("DELETE FROM products WHERE id = ?", (prod_id,))
                conn_del.commit()
                if delete_result.rowcount > 0:
                    logger.info(f"Successfully deleted purchased product record ID {prod_id}.")
                    # Schedule media directory deletion (using MEDIA_DIR from utils)
                    media_dir_to_delete = os.path.join(MEDIA_DIR, str(prod_id))
                    if os.path.exists(media_dir_to_delete): # Check sync before scheduling async
                        asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_delete, ignore_errors=True))
                        logger.info(f"Scheduled deletion of media dir: {media_dir_to_delete}")
                else: logger.warning(f"Product record ID {prod_id} not found for deletion after purchase.")
            except sqlite3.Error as e:
                logger.error(f"DB error deleting product record ID {prod_id}: {e}", exc_info=True)
                if conn_del and conn_del.in_transaction: conn_del.rollback()
            except Exception as e: logger.error(f"Unexpected error deleting product ID {prod_id}: {e}", exc_info=True)
            finally:
                if conn_del: conn_del.close()
            # --- END DELETE LOGIC ---
        # Final message
        final_message_parts = ["Purchase details sent above."]
        if sold_out_during_process:
             sold_out_items_str = ", ".join(item for item in sold_out_during_process)
             sold_out_note = lang_data.get("sold_out_note", "⚠️ Note: The following items became unavailable: {items}. You were not charged for these.")
             final_message_parts.append(sold_out_note.format(items=sold_out_items_str))
        leave_review_button = lang_data.get("leave_review_button", "Leave a Review")
        keyboard = [[InlineKeyboardButton(f"✍️ {leave_review_button}", callback_data="leave_review_now")]]
        await send_message_with_retry(context.bot, chat_id, "\n\n".join(final_message_parts), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        # Clear user context after successful purchase
        context.user_data['basket'] = []
        context.user_data.pop('applied_discount', None)
        return True
    else: # Purchase failed (likely due to DB error or all items sold out)
        if not sold_out_during_process: # Only send generic error if not handled by sold-out message
            await send_message_with_retry(context.bot, chat_id, error_processing_purchase_contact_support, parse_mode=None)
        return False


# --- MODIFIED Handler definition for confirm_pay ---
async def handle_confirm_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Pay Now' button press from the basket."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en']) # Get language data

    # 1. Clear expired items & get current basket context
    clear_expired_basket(context, user_id) # Sync call uses helper
    basket = context.user_data.get("basket", [])
    applied_discount_info = context.user_data.get('applied_discount')

    if not basket:
        await query.answer("Your basket is empty!", show_alert=True)
        return await user.handle_view_basket(update, context) # Use user module's function

    # 2. Calculate Final Total
    conn = None
    original_total = Decimal('0.0')
    final_total = Decimal('0.0')
    valid_basket_items_snapshot = []
    discount_code_to_use = None

    try:
        product_ids_in_basket = list(set(item['product_id'] for item in basket))
        if not product_ids_in_basket:
             await query.answer("Basket empty after validation.", show_alert=True)
             return await user.handle_view_basket(update, context)

        conn = get_db_connection() # Use helper
        # row_factory set in helper
        c = conn.cursor()
        placeholders = ','.join('?' for _ in product_ids_in_basket)
        # Use column names
        c.execute(f"SELECT id, price FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
        prices_dict = {row['id']: Decimal(str(row['price'])) for row in c.fetchall()}

        for item in basket:
             prod_id = item['product_id']
             if prod_id in prices_dict:
                 original_total += prices_dict[prod_id]
                 # Add item price from DB to snapshot for accurate final processing
                 item_snapshot = item.copy()
                 item_snapshot['price_at_checkout'] = prices_dict[prod_id]
                 valid_basket_items_snapshot.append(item_snapshot)
             else: logger.warning(f"Product {prod_id} missing during payment confirm user {user_id}.")

        if not valid_basket_items_snapshot:
             context.user_data['basket'] = []
             context.user_data.pop('applied_discount', None)
             logger.warning(f"All items unavailable user {user_id} payment confirm.")
             keyboard_back = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
             try: await query.edit_message_text("❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             except telegram_error.BadRequest: await send_message_with_retry(context.bot, chat_id, "❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             return

        final_total = original_total # Start with original
        if applied_discount_info:
            # Sync call
            code_valid, _, discount_details = user.validate_discount_code(applied_discount_info['code'], float(original_total))
            if code_valid and discount_details:
                final_total = Decimal(str(discount_details['final_total']))
                discount_code_to_use = applied_discount_info.get('code')
                context.user_data['applied_discount']['final_total'] = float(final_total)
                context.user_data['applied_discount']['amount'] = discount_details['discount_amount']
            else:
                final_total = original_total # Reset to original if code became invalid
                discount_code_to_use = None
                context.user_data.pop('applied_discount', None)
                await query.answer("Applied discount became invalid.", show_alert=True)

        if final_total < Decimal('0.00'):
             await query.answer("Cannot process negative amount.", show_alert=True)
             return

        # 3. Fetch User Balance
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        balance_result = c.fetchone()
        # Ensure balance is Decimal
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')

    except sqlite3.Error as e:
         logger.error(f"DB error during payment confirm user {user_id}: {e}", exc_info=True)
         kb = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
         await query.edit_message_text("❌ Error calculating total/balance.", reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
         return
    except Exception as e:
         logger.error(f"Unexpected error prep payment confirm user {user_id}: {e}", exc_info=True)
         kb = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
         try: await query.edit_message_text("❌ Unexpected error.", reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
         except telegram_error.BadRequest: await send_message_with_retry(context.bot, chat_id,"❌ Unexpected error.", reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
         return
    finally:
         if conn: conn.close()

    # 4. Compare Balance and Proceed
    logger.info(f"Payment confirm user {user_id}. Total: {final_total:.2f}, Balance: {user_balance:.2f}")

    if user_balance >= final_total:
        # Pay with balance
        logger.info(f"Sufficient balance user {user_id}. Processing with balance.")
        try:
            if query.message: await query.edit_message_text("⏳ Processing payment with balance...", reply_markup=None, parse_mode=None)
            else: await send_message_with_retry(context.bot, chat_id, "⏳ Processing payment with balance...", parse_mode=None)
        except telegram_error.BadRequest: await query.answer("Processing...")

        # Pass the final_total (Decimal) and snapshot
        success = await process_purchase_with_balance(user_id, final_total, valid_basket_items_snapshot, discount_code_to_use, context)

        if success:
            # Context cleared within process_purchase_with_balance
            try:
                 if query.message: await query.edit_message_text("✅ Purchase successful! Details sent.", reply_markup=None, parse_mode=None)
            except telegram_error.BadRequest: pass # Ignore edit error after success
        else:
            # If purchase failed, refresh basket view to show errors/changes
            await user.handle_view_basket(update, context)

    else:
        # Insufficient balance
        logger.info(f"Insufficient balance user {user_id}.")
        needed_amount_str = format_currency(final_total)
        balance_str = format_currency(user_balance)
        insufficient_msg = lang_data.get("insufficient_balance", "⚠️ Insufficient Balance! Top up needed.")
        top_up_button_text = lang_data.get("top_up_button", "Top Up Balance")
        back_basket_button_text = lang_data.get("back_basket_button", "Back to Basket")
        full_msg = (f"{insufficient_msg}\n\nRequired: {needed_amount_str} EUR\nYour Balance: {balance_str} EUR")
        keyboard = [
            [InlineKeyboardButton(f"💸 {top_up_button_text}", callback_data="refill")],
            [InlineKeyboardButton(f"⬅️ {back_basket_button_text}", callback_data="view_basket")]
        ]
        try: await query.edit_message_text(full_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest: await send_message_with_retry(context.bot, chat_id, full_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- END OF FILE payment.py ---

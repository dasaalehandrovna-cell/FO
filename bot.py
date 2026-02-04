# Updated bot.py

# ... [existing content of bot.py] ...

# Function to handle non-owner chat finance-related updates

def update_finance_message(chat_id, new_value):
    if not is_owner(chat_id):  # Check if the user is not the owner
        # Logic to update the existing main window message
        update_main_window_message(chat_id, new_value)
    else:
        # Existing logic for owner chats
        create_new_window_message(chat_id, new_value)

# Function to update the main window's message
def update_main_window_message(chat_id, new_value):
    # Logic to refresh the active window's text and keyboard
    current_message = get_current_window_message(chat_id)
    current_message.text = new_value
    current_message.reply_markup = get_keyboard_for_value(new_value)
    send_message(chat_id, current_message)
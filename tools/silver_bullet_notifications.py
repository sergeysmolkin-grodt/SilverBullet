import time
import datetime
import pytz
import requests
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# File to store message IDs for cleanup
script_dir = os.path.dirname(os.path.realpath(__file__))
MESSAGES_FILE = os.path.join(script_dir, 'message_ids.log')
LAST_CLEANUP_FILE = os.path.join(script_dir, 'last_cleanup.txt')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to set up file logging for PythonAnywhere
try:
    log_file = os.path.join(script_dir, 'bot.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    logger.info(f"File logging enabled to {log_file}")
except Exception as e:
    logger.warning(f"Could not set up file logging: {str(e)}")

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Trading Sessions in NY Time
SESSIONS_NY = [
    {"name": "Session 1", "start": "03:00", "end": "04:00"},
    {"name": "Session 2", "start": "10:00", "end": "11:00"},
    {"name": "Session 3", "start": "14:00", "end": "15:00"}
]

# Notification timing (minutes before session start)
NOTIFICATION_MINUTES_BEFORE = [30, 5, 0]  # Notify 30, 5 minutes before and at the start of session

# Test mode - set to False for production
TEST_MODE = False


def send_telegram_message(message):
    """Send a message to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not configured. Check your .env file.")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, data=data)
        response.raise_for_status()
        logger.info("Message sent successfully")

        # Store message_id for later cleanup
        try:
            result = response.json()
            if result.get('ok'):
                message_id = result['result']['message_id']
                with open(MESSAGES_FILE, 'a') as f:
                    f.write(f"{message_id}\n")
        except Exception as e:
            logger.error(f"Could not store message ID: {str(e)}")

        return True
    except Exception as e:
        logger.error(f"Failed to send message: {str(e)}")
        return False


def delete_telegram_message(message_id):
    """Delete a single message from Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "message_id": message_id}
    
    try:
        response = requests.post(url, data=data)
        if response.status_code == 200:
            logger.info(f"Successfully deleted message {message_id}")
            return True
        else:
            # It's common for deletion to fail for old messages (e.g., > 48h), so this is a warning.
            logger.warning(f"Could not delete message {message_id}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Exception while deleting message {message_id}: {str(e)}")
        return False


def clear_chat_history():
    """Reads all message IDs from the log file and deletes them one by one."""
    logger.info("--- Starting Daily Chat History Cleanup ---")
    if not os.path.exists(MESSAGES_FILE):
        logger.info("Message ID file not found. Nothing to clean.")
        return

    try:
        with open(MESSAGES_FILE, 'r') as f:
            message_ids = [line.strip() for line in f if line.strip()]
        
        if not message_ids:
            logger.info("No messages to delete.")
            # Clear the file anyway in case it just contains whitespace
            open(MESSAGES_FILE, 'w').close()
            return

        logger.info(f"Found {len(message_ids)} messages to delete.")
        
        deleted_count = 0
        failed_count = 0
        for message_id in reversed(message_ids): # Delete from newest to oldest
            if delete_telegram_message(message_id):
                deleted_count += 1
            else:
                failed_count += 1
            time.sleep(0.1)  # Brief pause to avoid hitting API rate limits

        logger.info(f"Cleanup complete. Deleted: {deleted_count}, Failed: {failed_count}.")

        # Clear the message ID file after processing
        open(MESSAGES_FILE, 'w').close()
        logger.info("Message ID file has been cleared.")

    except Exception as e:
        logger.error(f"An error occurred during chat history cleanup: {str(e)}")


def get_ny_and_utc3_now():
    """Get current time in NY and UTC+3."""
    utc_now = datetime.datetime.now(pytz.UTC)
    
    # New York time
    ny_tz = pytz.timezone('America/New_York')
    ny_now = utc_now.astimezone(ny_tz)
    
    # UTC+3 time (Moscow time)
    utc3_tz = pytz.timezone('Europe/Moscow')
    utc3_now = utc_now.astimezone(utc3_tz)
    
    return ny_now, utc3_now


def get_session_times_utc3(session, ny_date):
    """Convert NY session times to UTC+3 for a specific date."""
    ny_tz = pytz.timezone('America/New_York')
    utc3_tz = pytz.timezone('Europe/Moscow')
    
    # Parse session times
    start_hour, start_minute = map(int, session["start"].split(":"))
    end_hour, end_minute = map(int, session["end"].split(":"))
    
    # Create datetime objects for session start and end in NY time
    session_start_ny = ny_tz.localize(
        datetime.datetime(
            ny_date.year, ny_date.month, ny_date.day, 
            start_hour, start_minute, 0
        )
    )
    session_end_ny = ny_tz.localize(
        datetime.datetime(
            ny_date.year, ny_date.month, ny_date.day, 
            end_hour, end_minute, 0
        )
    )
    
    # Convert to UTC+3
    session_start_utc3 = session_start_ny.astimezone(utc3_tz)
    session_end_utc3 = session_end_ny.astimezone(utc3_tz)
    
    return session_start_utc3, session_end_utc3


def get_next_sessions(ny_now, utc3_now):
    """Get the next session times."""
    next_sessions = []
    
    # Check today's sessions
    for session in SESSIONS_NY:
        start_utc3, end_utc3 = get_session_times_utc3(session, ny_now.date())
        
        # If session hasn't ended yet
        if utc3_now < end_utc3:
            next_sessions.append({
                "name": session["name"],
                "start_ny": session["start"],
                "end_ny": session["end"],
                "start_utc3": start_utc3,
                "end_utc3": end_utc3
            })
    
    # If no sessions left today, check tomorrow
    if not next_sessions:
        tomorrow = ny_now.date() + datetime.timedelta(days=1)
        for session in SESSIONS_NY:
            start_utc3, end_utc3 = get_session_times_utc3(session, tomorrow)
            next_sessions.append({
                "name": session["name"],
                "start_ny": session["start"],
                "end_ny": session["end"],
                "start_utc3": start_utc3,
                "end_utc3": end_utc3
            })
    
    return next_sessions


def check_and_send_notifications():
    """Check if it's time to send notifications and send them if needed."""
    ny_now, utc3_now = get_ny_and_utc3_now()
    next_sessions = get_next_sessions(ny_now, utc3_now)
    
    for session in next_sessions:
        for minutes in NOTIFICATION_MINUTES_BEFORE:
            notification_time = session["start_utc3"] - datetime.timedelta(minutes=minutes)
            
            # If it's within a minute of notification time
            time_diff = (notification_time - utc3_now).total_seconds()
            if 0 <= time_diff <= 60:  # Within the next minute
                if minutes == 0:
                    # Session start notification
                    message = (
                        f"🔔 <b>Silver Bullet Trading Session Started</b> 🔔\n\n"
                        f"<b>{session['name']} is now active!</b>\n\n"
                        f"<b>Session Time (NY):</b> {session['start_ny']} - {session['end_ny']}\n"
                        f"<b>Session Time (UTC+3):</b> {session['start_utc3'].strftime('%H:%M')} - {session['end_utc3'].strftime('%H:%M')}\n\n"
                        f"Good luck with your trades!"
                    )
                    send_telegram_message(message)
                    logger.info(f"Sent session start notification for {session['name']}")
                else:
                    # Pre-session notification
                    message = (
                        f"⏰ <b>Silver Bullet Trading Session Alert</b> ⏰\n\n"
                        f"<b>{session['name']} starts in {minutes} minutes!</b>\n\n"
                        f"<b>Session Time (NY):</b> {session['start_ny']} - {session['end_ny']}\n"
                        f"<b>Session Time (UTC+3):</b> {session['start_utc3'].strftime('%H:%M')} - {session['end_utc3'].strftime('%H:%M')}\n\n"
                        f"Prepare your charts and get ready to trade!"
                    )
                    send_telegram_message(message)
                    logger.info(f"Sent {minutes}-minute notification for {session['name']}")


def send_test_notifications():
    """Send test notifications for all types to demonstrate functionality."""
    logger.info("Sending test notifications...")
    
    ny_now, utc3_now = get_ny_and_utc3_now()
    next_sessions = get_next_sessions(ny_now, utc3_now)
    
    if next_sessions:
        session = next_sessions[0]  # Use the next upcoming session for demo
        
        # 1. Send 30-minute notification example
        test_message = (
            f"⏰ <b>TEST: 30-Minute Notification Example</b> ⏰\n\n"
            f"<b>{session['name']} starts in 30 minutes!</b>\n\n"
            f"<b>Session Time (NY):</b> {session['start_ny']} - {session['end_ny']}\n"
            f"<b>Session Time (UTC+3):</b> {session['start_utc3'].strftime('%H:%M')} - {session['end_utc3'].strftime('%H:%M')}\n\n"
            f"Prepare your charts and get ready to trade!"
        )
        send_telegram_message(test_message)
        time.sleep(2)  # Brief pause between messages
        
        # 2. Send 5-minute notification example
        test_message = (
            f"⏰ <b>TEST: 5-Minute Notification Example</b> ⏰\n\n"
            f"<b>{session['name']} starts in 5 minutes!</b>\n\n"
            f"<b>Session Time (NY):</b> {session['start_ny']} - {session['end_ny']}\n"
            f"<b>Session Time (UTC+3):</b> {session['start_utc3'].strftime('%H:%M')} - {session['end_utc3'].strftime('%H:%M')}\n\n"
            f"Prepare your charts and get ready to trade!"
        )
        send_telegram_message(test_message)
        time.sleep(2)  # Brief pause between messages
        
        # 3. Send session start notification example
        test_message = (
            f"🔔 <b>TEST: Session Start Notification Example</b> 🔔\n\n"
            f"<b>{session['name']} is now active!</b>\n\n"
            f"<b>Session Time (NY):</b> {session['start_ny']} - {session['end_ny']}\n"
            f"<b>Session Time (UTC+3):</b> {session['start_utc3'].strftime('%H:%M')} - {session['end_utc3'].strftime('%H:%M')}\n\n"
            f"Good luck with your trades!"
        )
        send_telegram_message(test_message)
        
        # Final message explaining normal operation
        time.sleep(2)
        final_message = (
            f"✅ <b>Test Complete</b> ✅\n\n"
            f"The examples above show the notifications you will receive:\n"
            f"- 30 minutes before each session\n"
            f"- 5 minutes before each session\n"
            f"- At the start of each session\n\n"
            f"The script is now running and will send real notifications at the appropriate times.\n"
            f"To disable test mode, set TEST_MODE = False in the script."
        )
        send_telegram_message(final_message)
        
        logger.info("Test notifications sent successfully")
    else:
        logger.error("No upcoming sessions found to use for test notifications")


def main():
    """Main function to run continuously."""
    logger.info("Silver Bullet Notifications script started")
    
    # --- Daily Cleanup Logic ---
    try:
        today_str = datetime.date.today().isoformat()
        last_cleanup_date = ""
        if os.path.exists(LAST_CLEANUP_FILE):
            with open(LAST_CLEANUP_FILE, 'r') as f:
                last_cleanup_date = f.read().strip()

        if last_cleanup_date != today_str:
            logger.info(f"Last cleanup was on '{last_cleanup_date}', today is {today_str}. Running cleanup.")
            clear_chat_history()
            with open(LAST_CLEANUP_FILE, 'w') as f:
                f.write(today_str)
            logger.info(f"Updated last cleanup date to {today_str}.")
        else:
            logger.info("Chat history already cleaned up today.")
    except Exception as e:
        logger.error(f"Error during daily cleanup check: {str(e)}")
    
    # Set up end time for PythonAnywhere scheduled tasks (run for 23.5 hours)
    end_time = datetime.datetime.now() + datetime.timedelta(hours=23, minutes=30)
    logger.info(f"Script will run until: {end_time}")
    
    # Send startup message
    ny_now, utc3_now = get_ny_and_utc3_now()
    next_sessions = get_next_sessions(ny_now, utc3_now)
    
    startup_message = "🚀 <b>Silver Bullet Notification System Started</b> 🚀\n\n"
    startup_message += "<b>Upcoming Trading Sessions (UTC+3):</b>\n"
    
    for session in next_sessions[:3]:  # Show next 3 sessions
        startup_message += f"• {session['name']}: {session['start_utc3'].strftime('%Y-%m-%d %H:%M')} - {session['end_utc3'].strftime('%H:%M')}\n"
    
    send_telegram_message(startup_message)
    
    # Send test notifications if test mode is enabled
    if TEST_MODE:
        send_test_notifications()
    
    # Main loop
    try:
        while datetime.datetime.now() < end_time:  # Run until the scheduled end time
            check_and_send_notifications()
            time.sleep(60)  # Check every minute
        
        # Send shutdown message
        logger.info("Scheduled shutdown reached - exiting normally")
        send_telegram_message("ℹ️ <b>Info:</b> Silver Bullet notification system scheduled restart. Service continues without interruption.")
    
    except KeyboardInterrupt:
        logger.info("Script stopped by user")
    except Exception as e:
        logger.error(f"Error in main loop: {str(e)}")
        send_telegram_message(f"⚠️ <b>Error:</b> Silver Bullet notification system encountered an error: {str(e)}")


if __name__ == "__main__":
    main() 
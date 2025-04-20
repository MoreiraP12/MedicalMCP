import pyautogui
import time
import sys
import os  # Added for robust path handling
import pyperclip # Keep if you use it later

# --- Configuration Constants ---

# Get the directory where the script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Construct absolute path to the 'data' directory (assuming it's one level up)
# If 'data' is in the SAME directory as the script, change '../data' to 'data'
DATA_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'data'))

# --- Image Paths (using absolute paths) ---
# Ensure these files exist at the calculated paths
# Example: If script is in /Users/me/scripts/myscript.py, DATA_DIR will be /Users/me/data/
# INPUT_FIELD_IMAGE = os.path.join(DATA_DIR, 'claude_input_field.png') # Example
# SEND_BUTTON_IMAGE = None                                             # Example
NEW_CHAT_BUTTON_IMAGE = os.path.join(DATA_DIR, 'copy_button.png')
COPY_ICON_IMAGE = os.path.join(DATA_DIR, 'copy_button.png')          # Example

# --- Image Recognition Settings ---
# Confidence level for image matching (adjust 0.7-1.0). Crucial for reliability.
# Start high (e.g., 0.9) and lower if needed. Retina displays might need ~0.85.
IMAGE_CONFIDENCE = 0.85
# Use grayscale matching? Can be faster/more reliable sometimes. Test True/False.
USE_GRAYSCALE = False

# --- Timeouts and Delays (in seconds) ---
WAIT_AFTER_CLICK = 0.7           # Wait after clicking UI elements
# WAIT_FOR_RESPONSE = 15         # Placeholder: Time for AI to generate answer
# WAIT_AFTER_SEND = 1.0          # Placeholder: Wait after sending input
# WAIT_BEFORE_NEXT_QUESTION = 5  # Placeholder: Wait between processing questions
INITIAL_PAUSE = 2.0              # Initial delay before starting actions
LOCATE_TIMEOUT = 10              # Max seconds to search for an image before failing

# --- Main Script Logic ---
try:
    copy_location = pyautogui.locateCenterOnScreen(COPY_ICON_IMAGE)
    if copy_location:
        pyautogui.doubleClick(copy_location)
        print("Clicked 'Copy' button.")
        time.sleep(WAIT_AFTER_CLICK) # Brief pause after clicking
    else:
        print("'Copy' button image not found on screen, skipping.")
except Exception as e:
    print(f"Error trying to find/click 'Copy' button: {e}")

retrieved_answer = pyperclip.paste()
if not retrieved_answer:
    print("Warning: Clipboard seems empty after copy attempt.")
    retrieved_answer = "[Retrieval Failed - Clipboard Empty]" # Indicate failure

print(retrieved_answer)
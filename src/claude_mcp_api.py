import pyautogui
import time
import pyperclip
import csv
import os
import argparse # Import argparse for command-line arguments
from collections import OrderedDict # To keep unique questions in order

# --- Configuration (ADJUST THESE VALUES CAREFULLY!) ---
# CSV File Path is now handled by command-line arguments

# --- PyAutoGUI Settings ---
# Option 1: Coordinates (less reliable) - Find using pyautogui.displayMousePosition()
INPUT_FIELD_COORDS = (500, 800) # Example: Replace with actual coordinates
SEND_BUTTON_COORDS = None      # Example: Set to (x, y) if using button, None if using Enter

# Option 2: Image Recognition (more reliable but slower) - Take screenshots first!
INPUT_FIELD_IMAGE = '../data/claude_input_field.png' # Example: Path to input field image
SEND_BUTTON_IMAGE = None                     # Example: Path to send button image (if not using Enter)
NEW_CHAT_BUTTON_IMAGE = '../data/new_chat.png'       # <<< ADDED: Path to the new chat button image
COPY_ICON_IMAGE = '../data/copy_button.png'

# Timeouts (in seconds) - Adjust based on your system/Claude's speed
WAIT_AFTER_CLICK = 0.5        # Wait after clicking elements
WAIT_FOR_RESPONSE = 15        # IMPORTANT: Time for Claude to generate the answer
WAIT_AFTER_SEND = 1.0         # Wait briefly after sending
WAIT_BEFORE_NEXT_QUESTION = 5 # Wait time between processing questions

# Response Retrieval Method (CHOOSE AND IMPLEMENT ONE)
# Options: 'select_all', 'triple_click', 'drag_select' (you'll need coords for drag)
# Set to None if you haven't implemented retrieval yet.
RETRIEVAL_METHOD = 'icon' # Example: Try triple click first
RESPONSE_AREA_COORD = (500, 600) # Example: A coordinate within the response area (needed for triple_click/drag)
# DRAG_START_COORD = (500, 400)  # Example: Needed for 'drag_select'
# DRAG_END_COORD = (500, 750)    # Example: Needed for 'drag_select'

# --- End Configuration ---

def initialize_csv(filename):
    """Creates the CSV with headers if it doesn't exist."""
    # Check if the directory exists, create if not
    directory = os.path.dirname(filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

    # Initialize file if it doesn't exist or is empty
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Question', 'Answer']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        print(f"Initialized CSV file: {filename}")

def get_unique_questions(filename):
    """Reads the CSV and returns a list of unique questions."""
    questions = OrderedDict() # Use OrderedDict to preserve order roughly
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            # Ensure 'Question' column exists
            if 'Question' not in reader.fieldnames:
                 print(f"Error: Input CSV file '{filename}' must contain a 'Question' column.")
                 return []
            for row in reader:
                # Check if question is not empty or just whitespace
                question_text = row.get('Question', '').strip()
                if question_text:
                    questions[question_text] = None # Use dict keys for uniqueness
        return list(questions.keys())
    except FileNotFoundError:
        print(f"Error: Input file not found '{filename}'. Please check the path.")
        return []
    except Exception as e:
        print(f"Error reading input CSV file '{filename}': {e}")
        return []

def append_answer_to_csv(filename, question, answer):
    """Appends a new row with the question and answer to the CSV."""
    try:
        # Check if the file exists and has headers, add headers if not (relevant if output != input)
        file_exists = os.path.exists(filename)
        needs_header = not file_exists or os.path.getsize(filename) == 0

        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Question', 'Answer']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if needs_header:
                writer.writeheader()
                print(f"Initialized output CSV file: {filename}")
            writer.writerow({'Question': question, 'Answer': answer})
        # Shorten question for printing if it's very long
        print_question = (question[:47] + '...') if len(question) > 50 else question
        print(f"Appended answer for: {print_question}")
    except Exception as e:
        print(f"Error writing to output CSV file '{filename}': {e}")

def ask_claude_gui(prompt):
    """
    Uses PyAutoGUI to ask Claude a question and retrieve the answer.
    Returns the answer string or None if an error occurs.
    *** CRITICAL: You MUST adapt the response retrieval part below. ***
    """
    # Shorten prompt for printing if it's very long
    print_prompt = (prompt[:57] + '...') if len(prompt) > 60 else prompt
    print(f"Asking Claude via GUI: {print_prompt}")
    pyautogui.FAILSAFE = True # Move mouse to corner to abort

    try:
        # 1. Activate/Focus Claude Window (Implement a reliable method if needed)
        # Example: pyautogui.click(10, 10) # Click a known corner if window is always there
        # time.sleep(WAIT_AFTER_CLICK)

        # 2. Locate and Click Input Field
        # input_location = None
        """ if INPUT_FIELD_IMAGE:
            try:
                # Try locating the image
                input_location = pyautogui.locateCenterOnScreen(INPUT_FIELD_IMAGE)
                if input_location is None:
                    print("Input field image not found. Trying coordinates...")
                    input_location = INPUT_FIELD_COORDS # Fallback to coords
            except Exception as img_err:
                # Handle cases where screenshot capability might be missing (e.g., headless systems)
                print(f"Error finding input field image ({img_err}). Trying coordinates...")
                input_location = INPUT_FIELD_COORDS # Fallback to coords
        else:
            # Use coordinates if no image path is provided
            input_location = INPUT_FIELD_COORDS """

        """ if not input_location:
             print("Error: Could not determine input field location. Aborting question.")
             return None """

        # Click the determined location
        """ pyautogui.click(input_location)
        time.sleep(WAIT_AFTER_CLICK) """

        # Clear field first (optional, but good practice)
        # Use platform-specific hotkeys
        if os.name == 'nt': # Windows
            pyautogui.hotkey('ctrl', 'a')
        else: # macOS/Linux
            pyautogui.hotkey("command", "a", interval=0.1)

        """ time.sleep(0.1)
        pyautogui.press('delete') """
        time.sleep(0.1)

        # 3. Type the Question
        pyautogui.write("prompt", interval=0.01) # Smaller interval for faster typing
        time.sleep(WAIT_AFTER_CLICK)

        # 4. Send the Question
        send_location = None
        send_method_used = "Enter Key" # Default assumption
        if SEND_BUTTON_IMAGE:
             try:
                 send_location = pyautogui.locateCenterOnScreen(SEND_BUTTON_IMAGE)
                 if send_location:
                     pyautogui.click(send_location)
                     send_method_used = "Send Button Image"
                 else:
                     print("Send button image not found, trying Enter key.")
                     pyautogui.press('enter')
             except Exception as img_err:
                 print(f"Error finding send button image ({img_err}). Trying Enter key.")
                 pyautogui.press('enter')
        elif SEND_BUTTON_COORDS:
            pyautogui.click(SEND_BUTTON_COORDS)
            send_method_used = "Send Button Coords"
        else: # Default to pressing Enter
            pyautogui.press('enter')

        print(f"Question sent (using {send_method_used}). Waiting for response...")
        time.sleep(WAIT_FOR_RESPONSE) # Wait for Claude to generate

        # 5. Retrieve the Response (*** IMPLEMENT THIS CAREFULLY ***)
        print("Attempting to retrieve response...")
        pyperclip.copy("") # Clear clipboard first
        retrieved_answer = "[Response retrieval not implemented]" # Default value

        # Platform specific copy hotkey
        copy_key = 'ctrl' if os.name == 'nt' else 'command'
        if RETRIEVAL_METHOD == 'icon':
            print("Using Icon method...")
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

        elif RETRIEVAL_METHOD == 'select_all':
            print("Using Select All method...")
            pyautogui.hotkey(copy_key, 'a')
            time.sleep(WAIT_AFTER_CLICK)
            pyautogui.hotkey(copy_key, 'c')
            time.sleep(WAIT_AFTER_CLICK)
        elif RETRIEVAL_METHOD == 'triple_click':
             if RESPONSE_AREA_COORD:
                 print(f"Using Triple Click method at {RESPONSE_AREA_COORD}...")
                 pyautogui.tripleClick(RESPONSE_AREA_COORD)
                 time.sleep(WAIT_AFTER_CLICK)
                 pyautogui.hotkey(copy_key, 'c')
                 time.sleep(WAIT_AFTER_CLICK)
             else:
                 print("Error: RESPONSE_AREA_COORD not set for triple_click.")
                 return None
        elif RETRIEVAL_METHOD == 'drag_select':
             # if DRAG_START_COORD and DRAG_END_COORD:
             #     print(f"Using Drag Select method from {DRAG_START_COORD} to {DRAG_END_COORD}...")
             #     pyautogui.moveTo(DRAG_START_COORD)
             #     pyautogui.dragTo(DRAG_END_COORD, duration=1.0, button='left')
             #     time.sleep(WAIT_AFTER_CLICK)
             #     pyautogui.hotkey(copy_key, 'c')
             #     time.sleep(WAIT_AFTER_CLICK)
             # else:
             #     print("Error: Drag coordinates not set for drag_select.")
             #     return None
             print("Drag select method needs coordinates - currently disabled.")
             # Keep the placeholder value
        else:
            print("No valid RETRIEVAL_METHOD specified or implemented.")
            # Keep the placeholder value

        # Get text from clipboard only if a method was attempted
        if RETRIEVAL_METHOD in ['icon','select_all', 'triple_click', 'drag_select']:
             retrieved_answer = pyperclip.paste()
             if not retrieved_answer:
                 print("Warning: Clipboard seems empty after copy attempt.")
                 retrieved_answer = "[Retrieval Failed - Clipboard Empty]" # Indicate failure

        print("Response retrieval attempt finished.")
        return retrieved_answer

    except Exception as e:
        print(f"An error occurred during GUI automation: {e}")
        # Consider adding more specific error handling if needed
        return None # Indicate failure

# --- Main Execution ---
if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Automate asking questions to Claude GUI from a CSV and save answers.")
    parser.add_argument("--input-csv", required=True, help="Path to the input CSV file containing questions.")
    parser.add_argument("--output-csv", required=True, help="Path to the output CSV file where Q&A will be appended.")
    args = parser.parse_args()

    INPUT_CSV_FILE = args.input_csv
    OUTPUT_CSV_FILE = args.output_csv

    print("Starting Claude Q&A Automation...")
    print(f"Input CSV: {INPUT_CSV_FILE}")
    print(f"Output CSV: {OUTPUT_CSV_FILE}")

    # Initialize output CSV if needed (headers) - initialize_csv handles directory creation
    initialize_csv(OUTPUT_CSV_FILE) # Initialize output file specifically

    # Get questions from input CSV
    unique_questions = get_unique_questions(INPUT_CSV_FILE)

    if not unique_questions:
        print("No valid questions found in the input CSV file to process.")
    else:
        print(f"Found {len(unique_questions)} unique questions in '{INPUT_CSV_FILE}'.")
        print("Ensure the Claude application window is visible and ready.")
        print("Starting automation in 3 seconds (Press Ctrl+C in terminal to cancel)...")
        try:
            # Countdown timer
            for i in range(3, 0, -1):
                print(f"{i}...", end=' ', flush=True)
                time.sleep(1)
            print("Go!")
        except KeyboardInterrupt:
            print("\nCancelled by user.")
            exit()

        # Process each unique question
        for i, question in enumerate(unique_questions):
            print(f"\n--- Processing Question {i+1}/{len(unique_questions)} ---")
            answer = ask_claude_gui(question) # Ask Claude via GUI

            if answer is not None:
                # Append the original question and the retrieved answer to the output CSV
                append_answer_to_csv(OUTPUT_CSV_FILE, question, answer)
            else:
                print(f"Failed to get answer for question {i+1}.")
                # Optionally append a row indicating failure to the output CSV
                # append_answer_to_csv(OUTPUT_CSV_FILE, question, "[AUTOMATION FAILED]")

            # <<< ADDED: Attempt to click the New Chat button after processing >>>
            print("Attempting to start a new chat...")
            try:
                new_chat_location = pyautogui.locateCenterOnScreen(NEW_CHAT_BUTTON_IMAGE)
                if new_chat_location:
                    pyautogui.doubleClick(new_chat_location)
                    print("Clicked 'New Chat' button.")
                    time.sleep(WAIT_AFTER_CLICK) # Brief pause after clicking
                else:
                    print("'New Chat' button image not found on screen, skipping.")
            except Exception as e:
                print(f"Error trying to find/click 'New Chat' button: {e}")
            # <<< END ADDED SECTION >>>

            # Optional: Add a small delay between questions to avoid overwhelming the system or potential rate limits
            if i < len(unique_questions) - 1: # Don't wait after the last question
                 print(f"Waiting {WAIT_BEFORE_NEXT_QUESTION} seconds before next question...")
                 time.sleep(WAIT_BEFORE_NEXT_QUESTION)

    print("\nAutomation finished.")

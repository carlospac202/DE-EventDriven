import sys
import os
from event_driven import EventDriven


def main():
    # Create instances
    event = EventDriven()

    # Call methods from class
    event.process()


if __name__ == "__main__":
    main()  # Call the main function when the script is executed

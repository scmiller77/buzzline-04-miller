#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for storing sentiment data per author
import pandas as pd  # for datetime handling

# Import external packages
from dotenv import load_dotenv

# Import Matplotlib for live plotting
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("PROJECT_TOPIC", "buzzline_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("PROJECT_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store sentiment and timestamp for each author
author_data = defaultdict(list)  # stores [(timestamp, sentiment)] for each author
author_velocity = defaultdict(list)  # stores velocity (d/dt) of sentiment over time for each author

#####################################
# Set up live visuals
#####################################

# Create the plot figure and axis for live updates
fig, ax = plt.subplots()

# Turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
#####################################

def update_chart():
    """Update the live chart with the latest sentiment velocity data."""
    # Clear the previous chart
    ax.clear()

    # Create an empty list for the plot handles (lines) and labels
    plot_handles = []
    labels = []

    # Iterate over authors and plot their sentiment velocity over time
    for author, data in author_velocity.items():
        timestamps = [entry[0] for entry in data]
        velocities = [entry[1] for entry in data]

        # Plot the author's sentiment velocity over time as a line plot
        line, = ax.plot(timestamps, velocities, label=author)
        plot_handles.append(line)
        labels.append(author)

    # Set labels and title for the chart
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Sentiment Velocity (d/dt)")
    ax.set_title("Real-Time Sentiment Velocity - Stephen Miller")

    # Format the x-axis labels as hour:minute:second
    x_labels = [timestamp.strftime("%H:%M:%S") for timestamp in timestamps]
    ax.set_xticks(timestamps)  # Set the tick positions to the actual timestamps
    ax.set_xticklabels(x_labels, rotation=45, ha="right")  # Format the labels

    # Add the legend to the plot
    ax.legend(loc="upper left", handles=plot_handles, labels=labels)

    # Adjust layout for better spacing
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause to allow the chart to update
    plt.pause(0.01)

#####################################
# Function to process a single message
#####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the fields: author, sentiment, timestamp
            author = message_dict.get("author", "unknown")
            sentiment = message_dict.get("sentiment", 0)
            timestamp_str = message_dict.get("timestamp", "unknown")

            # Convert timestamp string to a datetime object for proper calculation
            timestamp = pd.to_datetime(timestamp_str)

            # Format the timestamp to show hour-minute-second for plotting (do not reassign it back to a string here)
            formatted_timestamp = timestamp.strftime("%H:%M:%S")

            logger.info(f"Message received from author: {author}, sentiment: {sentiment}, timestamp: {formatted_timestamp}")

            # Retrieve the previous sentiment for the author (if exists)
            previous_data = author_data[author][-1] if author_data[author] else None
            if previous_data:
                previous_timestamp, previous_sentiment = previous_data

                # Ensure the previous timestamp is a datetime object
                previous_timestamp = pd.to_datetime(previous_timestamp)

                # Calculate the derivative (velocity) of sentiment
                time_diff = (timestamp - previous_timestamp).total_seconds()  # time difference in seconds
                sentiment_diff = sentiment - previous_sentiment  # difference in sentiment
                velocity = sentiment_diff / time_diff if time_diff != 0 else 0  # velocity (d/dt)

                # Store the velocity in the author_velocity dictionary
                author_velocity[author].append((timestamp, velocity))

            # Store the current sentiment and timestamp for this author
            author_data[author].append((timestamp, sentiment))

            # Update the chart with the latest data
            update_chart()

            # Log the updated chart status
            logger.info(f"Chart updated successfully for author: {author}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()

    # Display the final chart
    plt.show()

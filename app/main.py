import argparse
import json
import os
from watchdog_status import determine_status,WatchdogStatus
from config import create_config


def main(config_path: str, input_data_paths: str[], ouput_data_path: str) -> None:
    """
    Loading the configuration
    """
    config = create_config(config_path)
    # step id from environment variable instead of configuration
    step_id = os.get("STEP")

    """
    Loading inputs from dependencies
    """
    print(f"Running watchdog for step {step_id}")
    print(f"Loading input data frame from {input_data_paths.len} inputs")
    all_successful = True
    all_positive = True
    max_timestamp = None
    for input_file in input_data_paths:
        with open(input_file, "r") as file:
            dependency = json.load(file)  # Load t
            step = dependency["step"]
            status = dependency["status"]
            timestamp = dependency["timestamp"]
            print(f"{step}: {status} at {timestamp}")
            if status != WatchdogStatus.SUCCEEDED:
                all_successful = False
            if status > 0:
                all_positive = False
            max_timestamp = max(max_timestamp, timestamp)
    print(f"Flags: all_successful={all_successful}, all_positive={all_positive}")
    print(f"Timestamps: from {max_timestamp} to {max_timestamp}")
    result = determine_status(config, all_successful, all_positive, max_timestamp)
    result["step"] = step_id

    """
    Triggering action
    """
    trigger_action(config, result)

    """
    Write output file
    """
    print(f"Writing result {result} to {ouput_data_path}")
    with open(ouput_data_path, "a") as file:
        file.write(json.dumps(result) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--input", required=True,  action='append') # append allows to add multiple --input args
    parser.add_argument("--output", required=True)
    args = vars(parser.parse_args())
    main(
        config_path=args["config"],
        input_data_paths=args["input"],
        output_data_paths=args["output"]
    )

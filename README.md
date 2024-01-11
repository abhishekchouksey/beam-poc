# README for Apache Beam POC Code

## Overview

This repository contains proof-of-concept (POC) code for Apache Beam, executed locally. The code is organized into incremental example folders named `example_{num}`, where `{num}` represents a sequential number indicating the progression of examples.

## Structure

- **Example Folders:** Each `example_{num}` folder contains specific data and scripts relevant to that stage of the project.
- **Data Flow Script:** The `data_flow_script.py` file is the primary script for executing Apache Beam tasks. This file contains docker commands (commented out) corresponding to each example.

## Running the Examples

### Using Docker

1. **Uncomment Docker Commands:** In `data_flow_script.py`, uncomment the Docker commands related to the example you want to run.
2. **Build Docker Image:** Execute the build command to create a Docker image for the selected example.
3. **Run Docker Container:** Once the image is built, run the Docker container to execute the example.

### Using Bare Metal

1. **Set Up Virtual Environment:**
   - Use Python's `venv` module to create a virtual environment.
   - Activate the virtual environment.
2. **Install Dependencies:**
   - Run `pip install -r requirements.txt` to install necessary dependencies.
3. **Execute Script:**
   - Run the script with `python3 data_flow_script.py` to execute the example on bare metal.

## Prerequisites

- Docker installed (for Docker execution)
- Python 3.x
- `venv` module (for bare metal execution)
- `pip` for installing dependencies

## Additional Notes

- Ensure Docker Desktop or Docker Engine is running if you choose to run the examples using Docker.
- For bare metal execution, ensure your Python environment matches the requirements of the Apache Beam version used in this project.
- Each example in the `example_{num}` folders is designed to demonstrate a specific functionality or concept in Apache Beam. Refer to the specific README in each folder for more details about the example.


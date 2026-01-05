#!/bin/bash

# SkyMonitor Ubuntu Setup Script
# This script installs Python 3, creates a virtual environment, and installs dependencies.

set -e

echo "--- Starting SkyMonitor Setup ---"

# 1. Update package lists
echo "Step 1: Updating packages..."
sudo apt-get update -y

# 2. Install Python 3 and venv
echo "Step 2: Installing Python 3, pip, and venv..."
sudo apt-get install -y python3 python3-pip python3-venv

# 3. Create a virtual environment
echo "Step 3: Creating virtual environment (.venv)..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# 4. Install dependencies
echo "Step 4: Installing dependencies from requirements.txt..."
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# 5. Environment configuration
echo "Step 5: Configuring environment variables..."
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "Created .env from .env.example. PLEASE EDIT IT with your Telegram credentials."
    else
        echo "WARNING: .env.example not found. Creating a blank .env."
        touch .env
    fi
else
    echo ".env file already exists."
fi

# 6. Set execute permissions (optional but good practice)
chmod +x sky_monitor.py

echo ""
echo "--- Setup Complete ---"
echo "Instructions:"
echo "1. Edit the .env file with your TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
echo "2. To run the monitor manually: .venv/bin/python sky_monitor.py"
echo "3. To use systemd, check sky_monitor.service and follow the comments inside."

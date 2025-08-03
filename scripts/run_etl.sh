#!/bin/bash
cd "$(dirname "$0")/.."
echo "🚀 Starting Financial ETL Job..."

# Set Java home
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-8.0.462.8-hotspot"
export PATH="$JAVA_HOME/bin:$PATH"

# Activate virtual env
# source "./venv310/Scripts/activate"
export VIRTUAL_ENV="$(pwd)/venv310"

# Export PySpark environment
export PYSPARK_PYTHON="./venv310/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="./venv310/Scripts/python.exe"
VENV_PYTHON="./venv310/Scripts/python.exe"

# Run your ETL script
# Run tests
echo "🧪 Running tests..."
"$VENV_PYTHON" -m pytest tests/

echo "🚛 Running main ETL..."
"$VENV_PYTHON" etl/main.py

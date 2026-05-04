#!/bin/bash
echo "Make sure Kafka is running on localhost:9092!"
echo "If not, start it with your WSL Kafka installation before running this."

# Ensure python venv
source venv/bin/activate

echo "Starting Flask Dashboard..."
python3 dashboard.py &
DASH_PID=$!

echo "Starting Spark Tactical Engine (SBT)..."
sbt run &
SBT_PID=$!

# Give Spark a few seconds to initialize its streaming logic
sleep 15 

echo "Starting Accurate Data Generator (Metrica CSV)..."
python3 real_data_generator.py &
GEN_PID=$!

echo "All components running!"
echo "Dashboard is at: http://localhost:5000"
echo "Press Ctrl+C to stop all."

trap "kill $DASH_PID $SBT_PID $GEN_PID; exit" INT
wait

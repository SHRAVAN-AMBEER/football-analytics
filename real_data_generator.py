import json
import time
import math
import csv
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'player_telemetry'

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("Connected to Kafka!")
except Exception as e:
    print(f"Error connecting to Kafka at {KAFKA_BROKER}: {e}")
    producer = None

PITCH_LENGTH = 105.0
PITCH_WIDTH = 68.0

def load_data(filename, team_name):
    players_info = {}
    frames = []
    
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        row1 = next(reader)
        row2 = next(reader) # Jersey
        row3 = next(reader) # Player IDs
        
        # Maps column index to player ID and jersey
        col_to_player = {}
        for i in range(3, len(row3)):
            if 'Player' in row3[i]:
                player_id = row3[i]
                jersey = row2[i]
                col_to_player[i] = {"id": f"{team_name}_{jersey}", "team": team_name}
        
        # Read the frames
        for row in reader:
            if not row or len(row) < 3: continue
            
            timestamp = float(row[2])
            frame_data = {
                "time": timestamp,
                "players": []
            }
            
            # Read players
            for col_idx, p_info in col_to_player.items():
                x_val = row[col_idx]
                y_val = row[col_idx+1]
                if x_val and y_val and x_val != 'NaN' and y_val != 'NaN':
                    x = float(x_val) * PITCH_LENGTH
                    y = float(y_val) * PITCH_WIDTH
                    
                    frame_data["players"].append({
                        "id": p_info["id"],
                        "team": p_info["team"],
                        "x": round(x, 2),
                        "y": round(y, 2),
                        "vel": 0.0 # Could compute based on history, but keeping it simple
                    })
            
            # Handle Ball from Home or Away dataset (it's at the end, so just read it)
            # The ball corresponds to the col that matches 'Ball'
            # Typically 2 columns before the end
            try:
                ball_x_str = row[-2]
                ball_y_str = row[-1]
                if ball_x_str and ball_y_str and ball_x_str != 'NaN' and ball_y_str != 'NaN':
                    frame_data["ball"] = {
                        "x": float(ball_x_str) * PITCH_LENGTH,
                        "y": float(ball_y_str) * PITCH_WIDTH,
                        "z": 0.0
                    }
            except:
                pass
                
            frames.append(frame_data)
            
    return frames

print("Loading dataset...")
home_frames = load_data('home.csv', 'Home')
away_frames = load_data('away.csv', 'Away')

# Ensure we just loop up to the minimum length (should be the same length)
min_frames = min(len(home_frames), len(away_frames))

# Store previous positions to calculate velocity
prev_positions = {}

print(f"Starting replay of {min_frames} frames...")
try:
    for i in range(min_frames):
        h_frame = home_frames[i]
        a_frame = away_frames[i]
        
        # Combine
        current_time = time.time()
        timestamp = int(current_time * 1000)
        
        raw_players = h_frame["players"] + a_frame["players"]
        players = []
        
        for p in raw_players:
            pid = p["id"]
            curr_x, curr_y = p["x"], p["y"]
            
            vel = 0.0
            if pid in prev_positions:
                prev_x, prev_y, prev_t = prev_positions[pid]
                dist = math.sqrt((curr_x - prev_x)**2 + (curr_y - prev_y)**2)
                dt = current_time - prev_t
                if dt > 0:
                    vel = dist / dt
            
            prev_positions[pid] = (curr_x, curr_y, current_time)
            
            players.append({
                "id": pid,
                "team": p["team"],
                "x": curr_x,
                "y": curr_y,
                "vel": round(min(vel, 12.0), 2) # Cap at sprinting speed
            })
        
        # Ball is in both, just take from home
        ball = h_frame.get("ball", {"x": 52.5, "y": 34.0, "z": 0.0})
        
        payload = {
            "timestamp": timestamp,
            "ball": ball,
            "players": players
        }
        
        if producer:
            try:
                producer.send(KAFKA_TOPIC, value=payload); print(f"[PRODUCER] Sent frame {i} at 25 FPS"); print(f"[PRODUCER] Sent frame {i} at 25 FPS")
            except Exception as e:
                pass
                
        # Simulated 25 FPS
        time.sleep(0.04)
        
except KeyboardInterrupt:
    print("Simulation stopped.")
finally:
    if producer:
        producer.close()

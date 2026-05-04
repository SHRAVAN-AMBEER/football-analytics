import json
import time
import random
import math
from kafka import KafkaProducer

# configuration
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
    # Proceed anyway to allow local testing
    producer = None

class Entity:
    def __init__(self, id, team, role, x, y):
        self.id = id
        self.team = team
        self.role = role
        self.x = x
        self.y = y
        self.vx = 0.0
        self.vy = 0.0

    def move(self, target_x=None, target_y=None, speed_limit=1.0):
        if target_x is not None and target_y is not None:
            dx = target_x - self.x
            dy = target_y - self.y
            dist = math.hypot(dx, dy)
            if dist > 0:
                self.vx += (dx / dist) * 0.2
                self.vy += (dy / dist) * 0.2
        
        # Add some random wander
        self.vx += random.uniform(-0.5, 0.5)
        self.vy += random.uniform(-0.5, 0.5)
        
        # Apply friction
        self.vx *= 0.8
        self.vy *= 0.8
        
        # Speed limit
        v = math.hypot(self.vx, self.vy)
        if v > speed_limit:
            self.vx = (self.vx / v) * speed_limit
            self.vy = (self.vy / v) * speed_limit
            
        self.x += self.vx
        self.y += self.vy
        
        # Boundaries (Pitch is 105 x 68)
        self.x = max(0, min(105, self.x))
        self.y = max(0, min(68, self.y))

# Initialize players
players = []
for i in range(1, 12):
    players.append(Entity(f"HOME_{i}", "Home", "Player", random.uniform(0, 50), random.uniform(0, 68)))
    players.append(Entity(f"AWAY_{i}", "Away", "Player", random.uniform(55, 105), random.uniform(0, 68)))

ball = Entity("BALL", "None", "Ball", 52.5, 34.0)
target_player = random.choice(players)
pass_timer = 0

print("Starting game simulation...")
try:
    while True:
        timestamp = int(time.time() * 1000)
        
        # Simulation Logic
        pass_timer += 1
        if pass_timer > 50: # Every 2 seconds change ball target
            target_player = random.choice([p for p in players if p.team == target_player.team])
            pass_timer = 0
            
        # Ball moves towards target player
        ball.move(target_player.x, target_player.y, speed_limit=2.5)
        
        # Players move
        for p in players:
            if p == target_player:
                p.move(ball.x, ball.y, speed_limit=1.5)
            else:
                p.move(speed_limit=1.0)
        
        # Create payload
        payload = {
            "timestamp": timestamp,
            "ball": {"x": round(ball.x, 2), "y": round(ball.y, 2), "z": 0.0},
            "players": [
                {"id": p.id, "team": p.team, "x": round(p.x, 2), "y": round(p.y, 2), "vel": round(math.hypot(p.vx, p.vy), 2)}
                for p in players
            ]
        }
        
        if producer:
            try:
                producer.send(KAFKA_TOPIC, value=payload)
            except Exception as e:
                pass
                
        time.sleep(0.04) # 25 FPS
except KeyboardInterrupt:
    print("Simulation stopped.")
finally:
    if producer:
        producer.close()

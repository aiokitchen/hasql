# Start chaos test cluster
chaos-up:
    sudo docker compose -f chaos/docker-compose.yml up -d --build

# Stop chaos test cluster and remove volumes
chaos-down:
    sudo docker compose -f chaos/docker-compose.yml down -v

# Show chaos cluster status via controller API
chaos-status:
    @curl -s localhost:8080/status | python3 -m json.tool

# Reset chaos cluster (unfreeze all, restart stopped)
chaos-reset:
    @curl -s -X POST localhost:8080/reset | python3 -m json.tool

# Start chaos controller
chaos-controller:
    cd chaos && uvicorn controller:app --port 8080

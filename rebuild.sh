#!/bin/bash
docker compose down
docker compose up -d --build
docker ps
docker logs exchanges_dashboard-scraper-1 -f
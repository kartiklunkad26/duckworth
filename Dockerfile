# Stage 1: Build React UI
FROM node:20-slim AS ui-build
WORKDIR /app/ui
COPY ui/package.json ui/package-lock.json* ./
RUN npm install
COPY ui/ ./
RUN npm run build

# Stage 2: Python backend + static files
FROM python:3.12-slim
WORKDIR /app

# Install system deps for psycopg2-binary and healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy source and project config, then install
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# Copy built React assets into static/
COPY --from=ui-build /app/ui/dist ./static/

# Copy schema for reference (used by docker-compose volume mount, not directly)
COPY schema.sql ./

EXPOSE 8000
CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]

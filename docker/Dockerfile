FROM postgres:17-bullseye

RUN apt-get update && apt-get install -y \
    postgis \
    postgresql-15-postgis-3 \
    postgresql-15-postgis-3-scripts \
    python3 \
    python3-pip \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/
RUN pip3 install psycopg2-binary>=2.9.9

# Copy configuration and scripts
COPY docker/seed-db.sh /usr/local/bin/
COPY scripts/ /usr/local/bin/
COPY database/init-db.sql /docker-entrypoint-initdb.d/

COPY data/ /data/

RUN chmod +x /usr/local/bin/seed-db.sh /usr/local/bin/seed_nca_atlas.py

CMD ["postgres"]
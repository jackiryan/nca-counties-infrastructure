FROM rust:1.75-slim as builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install martin

FROM postgres:15-bullseye

RUN apt-get update && apt-get install -y \
    postgis \
    postgresql-15-postgis-3 \
    postgresql-15-postgis-3-scripts \
    python3 \
    python3-pip \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/
RUN pip3 install -r /tmp/requirements.txt

# Copy Martin binary from builder stage
COPY --from=builder /usr/local/cargo/bin/martin /usr/local/bin/martin

# Copy configuration and scripts
COPY docker/start-services.sh /usr/local/bin/
COPY database/init-db.sql /docker-entrypoint-initdb.d/
COPY config/martin-config.yaml /etc/
COPY scripts/ /usr/local/bin/
COPY data/ /data/

RUN chmod +x /usr/local/bin/start-services.sh

ARG DOWNLOAD_DATA=false
RUN if [ "$DOWNLOAD_DATA" = "true" ] ; then python3 /usr/local/bin/download_sources.py --root ; fi
RUN python3 /usr/local/bin/seed_nca_atlas.py --root
RUN python3 /usr/local/bin/process_ncei_normals.py --root

EXPOSE 3000 5432

CMD ["/usr/local/bin/start-services.sh"]
FROM swift:latest

RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Download and compile SQLite
RUN wget https://sqlite.org/2025/sqlite-autoconf-3500300.tar.gz \
    && tar xzf sqlite-autoconf-3500300.tar.gz \
    && cd sqlite-autoconf-3500300 \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && ldconfig \
    && cd .. \
    && rm -rf sqlite-autoconf-3500300*

WORKDIR /app
COPY . .

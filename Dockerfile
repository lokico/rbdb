FROM swift:latest

RUN apt-get update && apt-get install -y \
    wget \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Download and compile SQLite
RUN wget https://sqlite.org/2025/sqlite-autoconf-3500300.tar.gz \
    && tar xzf sqlite-autoconf-3500300.tar.gz \
    && cd sqlite-autoconf-3500300 \
    && ./configure --prefix=/usr/local \
    && make \
    && make install \
    && ldconfig \
    && cd .. \
    && rm -rf sqlite-autoconf-3500300*

# Create module map for SQLite3
RUN mkdir -p /usr/lib/swift/linux \
    && echo "module SQLite3 [system] { header \"/usr/local/include/sqlite3_custom.h\"\n link \"/usr/local/lib/libsqlite3.so\"\n export * }" > /usr/lib/swift/linux/module.modulemap

WORKDIR /app
COPY . .

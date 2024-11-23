FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "src.main"]

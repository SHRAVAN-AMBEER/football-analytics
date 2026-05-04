#!/bin/bash
set -e

echo "Installing SBT..."
sudo apt-get update
sudo apt-get install apt-transport-https curl gnupg -yqq || true
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt -y

echo "Installing Spark..."
if [ ! -d "/opt/spark" ]; then
    wget -q https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
    tar xzf spark-3.5.0-bin-hadoop3.tgz
    sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
    rm spark-3.5.0-bin-hadoop3.tgz
fi

echo "Installing Python dependencies for Producer and Dashboard..."
sudo apt-get install python3-pip python3-venv -y
python3 -m venv venv
source venv/bin/activate
pip install kafka-python-ng flask flask-socketio eventlet paramiko

echo "Done!"

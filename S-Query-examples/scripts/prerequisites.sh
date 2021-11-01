#!/usr/bin/env bash

preventSubshell(){
  if [[ $_ != $0 ]]
  then
    echo "Script is being sourced"
  else
    echo "Script is a subshell - please run the script by invoking . script.sh command";
    exit 1;
  fi
}

preventSubshell
# Dependencies
sudo apt-get install -y wget apt-transport-https gnupg
# Add gpg key
wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -
# Get Ubuntu codename
VERSION=$(cat /etc/os-release | grep UBUNTU_CODENAME | cut -d = -f 2)
# Add repository and update
echo "deb https://adoptopenjdk.jfrog.io/adoptopenjdk/deb $VERSION main" | sudo tee /etc/apt/sources.list.d/adoptopenjdk.list
sudo apt-get update
# Install JDK-15 with hotspot JVM
sudo apt-get install -y adoptopenjdk-15-hotspot
# Add JAVA_HOME
echo "export JAVA_HOME=\$(readlink -f /usr/bin/javac | sed \"s:/bin/javac::\")" | sudo tee /etc/profile.d/java_home.sh
source /etc/profile.d/java_home.sh
# (Optional set java alternative)
#sudo update-alternatives --config java
#sudo update-alternatives --config javac

# Allow network traffic
sudo ufw allow proto tcp from 145.100.56.1/22 to any port 5701
# NTP server
sudo ufw allow proto udp from 145.100.56.1/22 to any port 123

# NTP client
sudo apt-get install -y ntp

#Edit /etc/ntp.conf:
#server 145.100.58.219
# Comment out the ntp pool lines
# Run: sudo systemctl restart ntp
# Run: sudo ntpq -np
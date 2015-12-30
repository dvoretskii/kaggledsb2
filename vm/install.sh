export DEBIAN_FRONTEND=noninteractive

# update
sudo apt-get update
sudo apt-get -y install postgresql postgresql-contrib
sudo apt-get --yes upgrade

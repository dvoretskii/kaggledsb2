export DEBIAN_FRONTEND=noninteractive

# update
sudo apt-get update
sudo apt-get -y install postgresql postgresql-contrib
sudo apt-get --yes upgrade
sudo -u postgres createuser --superuser vagrant
sudo -u postgres psql -c "ALTER USER vagrant WITH PASSWORD 'vagrant';"
sudo -u vagrant createdb kaggle
echo  'host    all             all             10.0.2.2/32            md5' | sudo tee -a /etc/postgresql/9.3/main/pg_hba.conf > /dev/null
echo "listen_addresses = '*'" | sudo tee -a /etc/postgresql/9.3/main/postgresql.conf > /dev/null
sudo service postgresql restart

# FLASK APP
# Thomas Instructions:
# https://github.com/InsightDataScience/flask-sample-app

# EC2 instance hosting app:
ssh -i "Connor-Dibble-IAM-keypair.pem" ubuntu@ec2-44-231-212-226.us-west-2.compute.amazonaws.com

# Clone repo with template:
git clone https://github.com/InsightDataScience/flask-sample-app
cd flask-sample-app
python3 -m venv venv --without-pip # create python vitrual environment from wtihin flask-sample-app
. venv/bin/activate # activate virtual environment

pip3 install Flask # install Flask in virtual env.
sudo apt install python3-flask

export FLASK_APP=hello.py # export FLASK_APP variable
flask run --host=0.0.0.0 # launch FLASK App

http://ubuntu@ec2-44-231-212-226.us-west-2.compute.amazonaws.com:5000


## example 2
export FLASK_APP=routes.py


## Run on port 80
export FLASK_APP=routes_port80.py
nohup flask run --host=0.0.0.0 --port=80 & 

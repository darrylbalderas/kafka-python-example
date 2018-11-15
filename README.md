# Kafka Python Example


Install python 

`brew install python3`


Create environment directory with python3

`python3 -m venv env `


Run the folder 

`source env/bin/activate`


Install requirements.txt

`pip install -r requirements.txt`


Start up a zookeeper and kafka instance

`./confluent-5.0.1/bin/confluent start`

Start up a consumer thread

`python consumer.pyt`

Start up a producer thread

`python producer.py`


Exit the environment

`deactivate`
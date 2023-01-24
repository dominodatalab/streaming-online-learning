python confluent_producer_malicious_url.py $1 0 $2 $3 &
echo 1
python confluent_producer_malicious_url.py $1 1 $2 $3 &
echo 2
python confluent_producer_malicious_url.py $1 2 $2 $3 &
echo 3
python confluent_producer_malicious_url.py $1 3 $2 $3 &
echo 4

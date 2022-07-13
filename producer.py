from kafka import KafkaProducer
import json
import os
import glob



def get_files(filepath):
    """
    Description:
       get the json files path from the file
    :param  filepath The file path that contains all json files
    :return list of json file path 
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def main():
    files = get_files('meta_data')
    # Initialize producer variable and set parameter for JSON encode
    producer = KafkaProducer(bootstrap_servers = 
    ['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # Send data in JSON format
    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            producer.send('input_topic', value = data, key =json.dumps(data['userId']).encode('utf-8'))
            print("Message Sent to input_topic")

    
if __name__ == "__main__":
    main()
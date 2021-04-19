import queue
import threading
import time
import requests
import csv

exitFlag = 0
successCount = 0

class simulate_sensor (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print("Starting " + self.name)
        simulate(self.q)
        print("Exiting " + self.name)
        global exitFlag
        exitFlag = 1


def simulate(q):
    dataset = []
    with open('dataset.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sensor_data = dict(row)
            dataset.append(sensor_data)

    for sensor_data in dataset:
        resp = publish_data(sensor_data)

        # if sensor data not published to server
        if(resp != 200):
            queueLock.acquire()
            data = q.put(sensor_data)
            print("%s processing %s" % ('simulate_sensor', sensor_data) )
            queueLock.release()
            print("size of queue %s " % (q.qsize(),) )
        else:
            global successCount 
            successCount += 1
        time.sleep(60)



class empty_buffer (threading.Thread):
    def __init__(self, threadID, name, q):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        print("Starting " + self.name)
        empty_buff(self.q)
        print("Exiting " + self.name)

        
def empty_buff(q):
    while not exitFlag:
        queueLock.acquire()
        if not workQueue.empty():
            sensor_data = q.get()
            queueLock.release()
            print("%s processing %s" % ('empty_buffer', sensor_data) )
            resp = publish_data(sensor_data)
            if(resp != 200):
                data = q.put(sensor_data)
                print("%s re-buffering %s" % ('empty_buffer', sensor_data) )
            else:
                global successCount 
                successCount += 1
        else:
            queueLock.release()
        time.sleep(5)

def publish_data(sensor_data):
    try:
        print(sensor_data)
        url = 'http://localhost:5001/live_data'
        resp = requests.post(url, json = sensor_data)
        return resp.status_code
    except requests.exceptions.RequestException as e:
       return 500

def get_status():
    global successCount
    status = {"Buffer size" : q.qsize(),
                "Success Count" : successCount}
    
    return status

queueLock = threading.Lock()
workQueue = queue.Queue()
threads = []

# Create new threads

thread = simulate_sensor(1, 'simulate_sensor', workQueue)
thread2 = empty_buffer(2, 'empty_buffer', workQueue)

thread.start()
thread2.start()

threads.append(thread)
threads.append(thread2)


# Wait for queue to empty
while not workQueue.empty():
   pass


# Wait for all threads to complete
for t in threads:
   t.join()
print("Exiting Main Thread")
from Queue import Queue
from threading import Thread
from utils import get_country_code,update_table
from time import time
import MySQLdb
import logging

TABLE = "googleimagefound"
DB_HOST = "localhost"
DB_NAME = "coutry_code"
USER = "root"
PASSWORD = ""


db = MySQLdb.connect(host = DB_HOST,db = DB_NAME,user = USER,passwd = PASSWORD)


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('requests').setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)

successfull = 0

class GetCountryCodes(Thread):
	def __init__(self,threadID,queue,cursor):
		Thread.__init__(self)
		self.threadID = threadID
		self.queue = queue
		self.cursor = cursor


	def run(self):
		global successfull
		while True:
			url,sql_id = self.queue.get()
			code  = get_country_code(url)
			if code is not None:
				update_db(self.cursor,code,sql_id)
				logger.info("Updated sql id:%s URL: %s ===> Code: %s",sql_id,url,code)
				successfull += 1
			else:
				logger.info("Failed to update country code for id: %s",sql_id)
			self.queue.task_done()



def get_urls(table):
	sql = 'select page_url,id from %s where ip= \"\";'%(table)
	cur = db.cursor()
	cur.execute(sql)
	data = cur.fetchall()
	cur.close()
	return data
 
def update_db(cursor,ip,sql_id):
	cur = cursor
	while True:
		try:
			cur.execute(update_table(ip,sql_id))
		except MySQLdb.OperationalError:
			db = MySQLdb.connect(host = DB_HOST,db = DB_NAME,user = USER,passwd = PASSWORD)
			cur = db.cursor()
			logger.info("Retrying to update ID: %s",sql_id)
			continue
		break


def main():
   ts = time()
   result = get_urls(TABLE)
   queue = Queue()
   # Create 8 worker threads
   for x in range(8):
       worker = GetCountryCodes(x,queue,db.cursor())
       worker.daemon = True
       worker.start()

   for data in result:
       logger.info("Queueing %s",data[0])
       queue.put((data))
   # Causes the main thread to wait for the queue to finish processing all the tasks
   queue.join()	
   success_rate = (successfull / float(len(result)))*100
   logger.info('Took {}'.format(time() - ts))
   logger.info("Success Rate: %s %%",success_rate)

if __name__ == "__main__":
	main()

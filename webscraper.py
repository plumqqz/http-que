import psycopg2
import psycopg2.extras
import json
import requests
import time

conn = psycopg2.connect("dbname='work' user='postgres' host='localhost' password='root'")
cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

while 1 :
  cr.execute("""select * from que.get()""") #start transaction and get message
  row = cr.fetchone()
  if not row: #if there is not any message, just sleep a little
    conn.commit() # we must commit if we don't want 'idle in transaction' backends
    time.sleep(1)
    continue

  has_errors = 0
  try:
    resp = requests.get(row['url']) #try to get; here we can realize more sophisticated logic
                                    # - handle put requests, SOAP and so one, here we just do simple get
  except requests.exceptions.RequestException as e:
    print e             # in real application it is much more desirable to have a real logging
    has_errors = str(e)
    
  if not has_errors and resp.status_code == requests.codes.ok :
     cr.execute("""select que.done(%s, %s, %s)""", (row['id'], json.dumps(dict(resp.headers)), psycopg2.Binary(resp.content)) )
  elif not has_errors:
     cr.execute("""select que.fail(%s,%s)""", (row['id'], 'Bad status code:' + resp.status_code) )
  else:
     cr.execute("""select que.fail(%s,%s)""", (row['id'], has_errors) )
  #end if

  conn.commit() #do commit to save changes

#end while


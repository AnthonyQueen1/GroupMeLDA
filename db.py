import pymysql.cursors

connection = pymysql.connect(host='localhost',
                             user='root',
                             password='theradio',
                             db='groupme',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

def get_result(id):
  with connection.cursor() as cursor:
    # if id == 'all':
    if False:
      sql = "SELECT message_id, word, count, likes FROM groupname  NATURAL JOIN groupmessages NATURAL JOIN wordcount"
      cursor.execute(sql)
    else:
      sql = "SELECT message_id, word, count FROM groupname NATURAL JOIN groupmessages NATURAL JOIN wordcount WHERE word NOT IN (  SELECT common_word FROM commoncase  ) AND group_id = %s"
      cursor.execute(sql, (str(id)))
    result = cursor.fetchall()
    if not result: raise ValueError ('ERROR: Group with id: ' + id +  ' is not in the database')
    return  result

async def get_message_counts(id):
  result = get_result(id)
  for tup in result: tup['count'] = int(tup['count'])
  return result

async def get_vocab(group_id, size):
  with connection.cursor() as cursor:
    sql = "SELECT wc.word, sum(wc.count) AS count FROM wordcount wc NATURAL JOIN groupmessages gm WHERE wc.word NOT IN (  SELECT common_word FROM commoncase  ) AND gm.group_id = %s GROUP BY wc.word ORDER BY count DESC LIMIT %s"
    cursor.execute(sql, (str(group_id), size))
    result = cursor.fetchall()
    return result

def close_con():
  connection.close()

# formatter.py
import onlinelda
import db
import numpy

VOCAB_SIZE      = 300
BATCH_SIZE      = 128
CLUSTERS        = 100

async def get_vocab(group_id):
  db_vocab = await db.get_vocab(group_id, VOCAB_SIZE)
  vocab = dict()
  for tup in db_vocab:
    vocab[tup['word']] = len(vocab)
  return vocab

def get_docs(allmessages, current_index, amt, vocab):
  wordids = list()
  wordcts = list()
  msg_count = i = current_index
  while i < len(allmessages) and msg_count < current_index + amt:
    m_id = allmessages[i]['message_id']
    temp_wordids = list()
    temp_wordcts = list()
    while i < len(allmessages) and allmessages[i]['message_id'] == m_id :
      if allmessages[i]['word'] in vocab:
        temp_wordids.append( vocab[allmessages[i]['word']] )
        temp_wordcts.append( allmessages[i]['count'] )
      i+=1
    msg_count+=1
    wordids.append(temp_wordids)
    wordcts.append(temp_wordcts)
  return (wordids, wordcts)

async def start_lda(group_id):
  print('starting lda')
  allmessages = await db.get_message_counts(group_id)
  vocab = await get_vocab(group_id)
  return await run_lda(allmessages, vocab)

async def run_lda(allmessages, vocab):
  print('running lda')
  # The number of documents to analyze each iteration
  batchsize = BATCH_SIZE
  # The total number of documents 
  D = len(allmessages)
  # The number of topics
  K = CLUSTERS

  olda = onlinelda.OnlineLDA(vocab, K, D, 1./K, 1./K, 1024., 0.7)


  for iteration in range(0, D-batchsize, batchsize):
    (wordids, wordcts) = get_docs(allmessages, iteration, batchsize, vocab)
    (gamma, bound) = olda.update_lambda_docs(wordids, wordcts)
    perwordbound = bound * batchsize / (D * sum(map(sum, wordcts)))

    if (iteration % 10 == 0):
      print(iteration )
      # numpy.savetxt('./data/lambda-%d.dat' % iteration, olda._lambda)
      # numpy.savetxt('./data/gamma-%d.dat' % iteration, gamma)
  return find_topics(list(vocab.keys()), olda._lambda)

def find_topics(vocab, testlambda):
  topics = list()
  for k in range(0, len(testlambda)):
    lambdak = list(testlambda[k, :])
    lambdak = lambdak / sum(lambdak)
    temp = zip(lambdak, range(0, len(lambdak)))
    temp = sorted(temp, key = lambda x: x[0], reverse=True)
    
    # for printing results to console
    # print ('topic %d:' % (k))
    current_topic = list()
    for i in range(0, 50):
      current_topic.append([vocab[temp[i][1]], temp[i][0]])

      # for printing results to console
      # print ('%20s  \t---\t  %.4f' % (vocab[temp[i][1]], temp[i][0]))
    topics.append(current_topic)
  print('finished lda, returning topics')
  return topics



# jfor testing, uses 'SAC' group
def main():
  import asyncio
  loop = asyncio.get_event_loop()
  loop.run_until_complete(start_lda(10311087))

if __name__ == "__main__":
  main()

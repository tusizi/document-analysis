# coding: utf-8
# 使用gensim写的lda，但是没有可以使用的分词结果
import gensim

from gensim import corpora

documents = ["Human machine interface for lab abc computer applications",
             "A survey of user opinion of computer system response time",
             "The EPS user interface management system",
             "System and human system engineering testing of EPS",
             "Relation of user perceived response time to error measurement",
             "The generation of random binary unordered trees",
             "The intersection graph of paths in trees",
             "Graph minors IV Widths of trees and well quasi ordering",
             "Graph minors A survey"]

texts = [[word for word in document.lower().split()] for document in documents]

dictionary = corpora.Dictionary(texts)
corpus = [dictionary.doc2bow(text) for text in texts]

lda_result = gensim.models.ldamodel.LdaModel(corpus=corpus, id2word=dictionary, num_topics=3)

lda_result.print_topics(2)

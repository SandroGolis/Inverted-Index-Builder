# Inverted Index Builder
Inverted index builder for future query processing on wikipedia dump file using hadoop mapreduce

There are many ways to make the inverted index and most of them involve more
than one passes over the data (i.e. more than one map reduce jobs). Moreover, it is
easy to write a not scalable code. In addition, inverted index tables tend to grow very
large, which may cause performance problems. In this project, we wanted to address
this issues and, therefore we had several main goals:

1. Make a single map-reduce job , that outputs the inverted index. The benefit in
having a single job is easier maintenance and having a more extendable code for
the future.

2. Sort the inverted index table not only by terms (which is done automatically by
the map-reduce framework), but also by page ids. So, the list of pages that is
associated with a specific term, should be sorted by page ids. This is important
for the efficiency of the query processing part. For example, when an AND query
is processed and we need to find a conjunction between two associated lists, we
will benefit from two sorted lists and will need to make only one pass over them
to find the conjunction.

3. Scalability. The easiest way to accomplish the sorting that is described in the
previous bullet, is to sort each list in the reduce function. So, the reducer that
receives as an input a term and a list of associated document ids, would output a
sorted list for that term. While this approach is easy to implement, it is not
scalable. The most frequent terms will cause the reducer to sort bigger lists and
the load on the reducer would grow with the addition of data. Instead, we have
overwritten the frameworks partitioner and comparator to achieve a scalable
solution for the sorting.
  - The mapper outputs [Term,PageId] as a composite key. The pageId is
    injected only for the sort purpose.
  - Custom Partitioner - makes sure all the records with the same term end up
    at the same reducer.
    Partition([Term, PageId]) = hash(Term) mod N
  - Custom Comparator - makes sure that the reducer receives list of values
    that correspond to the same term only. Without the custom comparator,
    since we have the composite key [Term,PageId], 2 terms from
    different pages would have gone to different reducers.
  - This steps are taking the advantage of map-reduce frameworks sorting
    properties and cause the secondary sorting by pageId.

4. Compact size - we addressed the following language processing techniques:
  - Stemming. We used the opensource Porter Stemmer to normalize the
    close variants of the words to a single form.
  - Stop words removal. We used a list of ~120 english stop words from the
    internet. In addition we ran a wordCount task on the wikipedia dump file
    and handpicked additional ~40 most frequent words to be added to our
    stop list. The words were chosen according to our intuition of less
    informative words.
    
Our inverted index is of the following format:
Term,DF     [PageId,Offset,TF,(idx_1,...,idx_TF)],..., [..]

Df = document frequency

TF = term frequency

Idx = index of the beginning of the term inside the document


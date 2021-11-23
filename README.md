# amba-analysis-worker-pubfinder

The Pubfinder component retrieves publication metadata and, if possible, abstracts for publications that are linked to an event.
The Pubfinder component is implemented in python by using the amba-event-streams package.

The process of retrieving publication data can be seen in Figure 1. The Pubfinder is built around its main worker utilizing individual sources which are integrated similar. This architecture allows adding, remove or modify sources since their only difference is in the source retrieval and transformation. This means the Kafka and database connections are maintained by the main worker, while the sources are separated to only retrieve information for a given DOI.

![pubfinder](https://user-images.githubusercontent.com/84507772/142741031-69ee7265-1411-4612-805c-5e1ea93823a1.png)
Process Overview of Retrieving Publication Data

All sources have their own retrieval mechanisms and data
transformations, as well as API limits that must be regarded. The
following sources are used: ambalytics , CrossRef , OpenAIRE , Semantic
Scholar and Meta Tags. CrossRef, OpenAIRE and Semantic Scholar are
service that offer APIs, while Meta Tags are Dublin Core HTML header
meta tags that may be embedded in the respective publication web page of
the publisher. Table 1 shows the
individual fields that can be retrieved by a source, as well as their
rate limit and URL. Ambalytics is the internal database (see
Figure 1), is technically not a real source and therefore
not listed in the table. Even though CrossRef and Meta Tags are not rate
limited, the number of requests per time unit is limited to comply with
the recommended usage of the respective APIs.

Since all sources are needed for a full check, the maximum throughput is
limited by the slowest source or in this case, the source with the
lowest rate limit. However, using this rate limit as base for the
worst-case calculation 100 publications per 5 minutes will amount to
28800 per 24h period. Each source is integrated similarly to the main
python program, but runs it own threads independently. In order to allow
fast and parallel workloads, the process can either be implemented
asynchronously allowing to use the await syntax to avoid blocking the
thread or using queues and multi-threading. Unfortunately, the support
of asynchronous python libraries is still limited and running blocking
functions in asynchronous environment would defeat its workings.
Following, the Pubfinder is developed based on the *queue architecture
pattern*.

| Source         |     CrossRef     |      OpenAIRE       |    Semantic Scholar     | Meta Tags |
|:---------------|:----------------:|:-------------------:|:-----------------------:|:---------:|
| URL            | api.crossref.org | develop.openaire.eu | api.semanticscholar.org |    \-     |
| Rate Limit     |        \-        |       3600/h        |         100/5m          |    \-     |
| Title          |        x         |          x          |            x            |     x     |
| Authors        |        x         |          x          |            x            |     x     |
| Year           |        x         |          x          |            x            |     x     |
| Type           |        x         |         \-          |           \-            |    \-     |
| Citation Count |        x         |         \-          |            x            |     x     |
| Pub Date       |        x         |          x          |           \-            |     x     |
| Abstract       |        x         |          x          |            x            |     x     |
| References     |        x         |         \-          |           \-            |    \-     |
| Citations      |        x         |         \-          |            x            |     x     |
| Publisher      |        x         |          x          |            x            |     x     |
| Field Of Study |        x         |          x          |            x            |     x     |
| License        |        x         |         \-          |           \-            |    \-     |

Publication Meta Data Sources

Queues allow each source to run indecently and multi-threaded while
reading and writing events into and from queues. A worker queue for each
source contains all events, and all sources share a result queue. The
queue is implemented as *deque* allowing for thread-safe operation and
adding references and citations. Using a deque allows to add new DOI’s
to the front while adding references and citations to the end, resulting
in earlier retrieval for the more important event publications. The
management of the queues is done in the Pubfinder, checking for finished
publications and adding an element to the correct source if more data is
required.

Since a publication resolution runs through each source until it is
complete and implements different limits and speeds, the order is
critical for overall speed. The API with the lowest rate limit should be
last. This order of sources reduces the total request count affecting
the limit since publications are more likely to be complete before and
therefore reducing the count.

Further, it will not slow down the processing. Each source uses the same
base way of finding data. The given DOI will request the data in a
defined way while ensuring API limits are kept. Once data is retrieved,
the needed data is extracted and mapped into the internal publication
structure. If a source adds data, it will also add its source. Finally,
the publication data is added to the event, and the event is added to
the result queue.

Once a publication is complete or run through all sources and contains
enough data, it is stored in PostgreSQL. Then publication data is added
to the event, the event state is changed to *linked*, and finally, the
event is sent to Kafka.

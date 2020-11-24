# WikidataHistoryExtractor


The script “wikidata_history_extractor.py” downloads the dumped edit history of Wikidata accumulated up to a certain date and transforms it into a time stream of (semantic) triple operations.

Following the RDF-model, a semantic triple describes a semantic relation between two Wikidata items. 
A triple operation, in turn, documents when a certain triple has been added to the knowledge base of Wikidata.

I wrote this script for my master's thesis (https://github.com/rlafraie/masters-thesis) where I 
focussed on inductive techniques of knowledge graph embedding and elaborated an evaluation framework to assess their predictive power throughout the evolution of a knowledge graph. In this sense, I needed an evolving knowledge graph which is based on factual data and represented by a time stream of triple operations.

“wikidata_history_extractor.py” requires two inputs. That is:

1) The number of cores granted for the extraction process (used for parallel processing of the history dumps).

2) The dump date of the Wikidata history. As the dumps are frequently released, please have a look at 
https://dumps.wikimedia.org/wikidatawiki/ for the seven latest dump dates. 
Be aware that not all of these dump dates release the Wikidata history. History dumps considered by the script follow the pattern “wikidatawiki-[dump date]-pages-meta-history*.xml*.bz2”. So before using this script, look into the listed paths at the aforementioned link to assure that such files are included.

I executed this script for the Wikidata history dumped on 1st May 2020 and obtained a time stream of approx. 9.3 million triple operations. Accordingly, I named this dataset Wikidata9M and published it at https://github.com/rlafraie/Wikidata9M. As the script requires to download and process a huge amount 
of data, you may use the Wikidata9M dataset if your use case does not require the change history that has occurred in the meantime.

# Fusion-j Overview
## A helper lib modeled on Solrj. 
This is meant to help access to Fusion 'things'. Initially this is focused on indexing and querying. 

## Eventually it may expand to helping with:
- Fusion configuration
- Fusion best practices 
- data transfer at scale (efficiently)
- ...

## Theoretical structure

- one client per Fusion instance (many Fusion apps, collections,...)
- perform authentication on construction (new client)
  - buildHttpClient()
  - buildCredentialsProvider() 
  - save and cache auth info to handle session timeout, also adding pooled clients (coming soon?)
- Fusion Info and stats
  - getFusionInformation()
  - getFusionVersion()
- 
- query: (app, collection, query params[q:'...', ], handler='select')
- index: (app, collection, index params[docs:[], ], handler='select')


## Diff friendly exports
### idex pipeline
notes:
* leave id? name of pipeline, probably keep
* id == secretSourceid?? assume so for now (revisit?) 
  * remove secret id 
* id naming tool/process? human readable stages etc, 
  * does fusion step in IDs? can we humanize ids and keep them around?
  * Users? set id to something like: realm-userid-...? make it guessable?
* Datasources
  * verify access??
  * solr_base_url?
  * 



# NOTE: Fusion 4.1.x 
Objects.json might be different structure, and may need processing changes

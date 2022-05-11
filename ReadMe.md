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

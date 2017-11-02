# elasticsn

This is an ETL like program aimed to parse and index (in Elasticsearch) the [transcripts](https://www.grc.com/securitynow.htm) from Leo Laporte & Steve Gibson's podcast [Security now](https://twit.tv/shows/security-now).

By doing so, it is possible to search for terms, phrases, ... within the text as well as extracting statistics.

**e.g: When was the first time someone said "complexity is the enemy of security" in the show? On which episodes has been it repeated?**

*/tmp/phrase.json:*

```javascript
{
  "script_fields": {
    "airedOn": {
      "script": {
        "source": "new Date(doc['header.date'].getValue())"
      }
    }
  },
  "_source": ["header.number", "header.audio"],
  "sort": [
    {
      "header.date": {
        "order": "asc"
      }
    }
  ],
  "query": {
    "nested": {
      "path": "text",
      "query": {
        "match_phrase": {
          "text.line": "complexity is the enemy of security"
        }
      },
      "inner_hits": {}
      }
    }
  }
}
```

*Client request using CURL:*

```bash
curl -u USER:PASS \
    -X GET https://HOST:PORT/securitynow/episode/_search \
    -d @/tmp/phrase.json | \
    jq '.hits.hits[] | { episode: ._source.header.number, airedOn: .fields.airedOn[0], saidBy: .inner_hits.text.hits.hits[0]._source.speaker, phrase: .inner_hits.text.hits.hits[0]._source.line}'
```

*...and BOOM!:*

![results](https://i.imgur.com/F3tE5AH.png)

**e.g: Who are the show hosts? Who is intervenes often? Who is more talkative?**

Just use Kibana!

![kibana stats](https://i.imgur.com/13uosUr.png)

Both Leo and Steve are the main hosts and they spoke similar numbers of phrases per show, however, reviewing word count by author it is obvious that Steve leads
the discurse during each session.

## Indices created by the ETL Scala program

All episodes relevant information is stored under the *securitynow* index. However, there is a second index, *securitynow_words*, where each spoken word is stored as
a keyword in order to perform aggregations in Kibana.

### *securitynow* index

As stated above, this index should be enough to perform analysis and get all the information from search queries. In fact, it contains two mappings (*episode* and *episodeLine*) of which *episode*
keeps the information of all the aired shows with all the interventions. It is just an indexed version of the parsed transcription, check its mapping:

```javascript
"episode": {
        "properties": {
          "audioURL": {
            "type": "text"
          },
          "date": {
            "type": "date"
          },
          "header": {
            "properties": {
              "audio": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "date": {
                "type": "long"
              },
              "hosts": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "number": {
                "type": "long"
              },
              "site": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "title": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          "notesURL": {
            "type": "text"
          },
          "number": {
            "type": "integer"
          },
          "speakers": {
            "type": "text"
          },
          "text": {
            "type": "nested",
            "properties": {
              "line": {
                "type": "text"
              },
              "speaker": {
                "type": "keyword"
              }
            }
          },
          "title": {
            "type": "text"
          }
        }
      }
```

Note how each line of dialog is stored within the nested object `text` so Elasticsearch can query those lines as in the example shown at the beginning of this document.

As it would happen with the *securitynow_words* index, the *episodeLine* mapping serves Kibana including some `keyword` fields.

## TODO

- Factor `Upload` object into a generic indexer which should allow modularization when creating different views, in the form of indices and mappings.
- Unit tests.
- Add a "listener" aimed to make of this ETL a stream endpoint which could keep the index updated with the new shows.

from uuid import uuid4
from elasticsearch import Elasticsearch

ES_INDEX = 'livros'

es = Elasticsearch()

if __name__ == '__main__':
    ##### CRUD OPERATIONS ####

    document = {
        "nome": "Do Mil ao Milhão. Sem Cortar o Cafezinho",
        "valor": 25.50,
        "autor": "Thiago Nigro",
        "data_lancamento": "2018-11-10",
        "tags": [
            "finanças", "investimentos", "dinheiro", "empreendedorismo"
        ]
    }

    ##### CREATE INDEX ####
    es.indices.create(
        index=ES_INDEX,
        body={
            "settings": {"number_of_shards": 4},
            "mappings": {
                "properties": {
                    "nome": {
                        "type": "text"
                    },
                    "valor": {
                        "type": "float"
                    },
                    "autor": {
                        "type": "text"
                    },
                    "data_lancamento": {
                        "type": "date"
                    },
                    "tags": {
                        "type": "text"
                    }
                }
            },
        },
        ignore=400,
    )

    document_id = uuid4()

    ##### CREATE DOCUMENT ####
    response = es.create(index=ES_INDEX, id=document_id, body=document)
    print(response)

    ##### GET DOCUMENT ####
    response = es.get(ES_INDEX, id=document_id)
    print(response)

    ##### UPDATE DOCUMENT ####
    document['tags'].append("mercado financeiro")
    response = es.index(ES_INDEX, id=document_id, body=document)
    print(response['result'])

    ##### QUERY ####
    response = es.search(index=ES_INDEX, body={'query': {'match_all': {}}})
    print(response)

    ##### DELETE DOCUMENT ###
    response = es.delete(ES_INDEX, id=document_id)
    print(response['result'])

    ##### DELETE INDEX ####
    es.indices.delete(ES_INDEX, ignore=[400, 404])

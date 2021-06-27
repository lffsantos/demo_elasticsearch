from uuid import uuid4
from elasticsearch import Elasticsearch


class ElasticSearchService:
    es = None

    def __init__(self):
        self.es = Elasticsearch()

    def create_index(self, index: str):
        self.es.indices.create(index=index, ignore=400)

    def delete_index(self, index: str):
        self.es.indices.delete(index, ignore=[400, 404])

    def create(self, index: str, doc_id: str, doc: dict) -> dict:
        response = self.es.create(index=index, id=doc_id, body=doc)
        return response

    def get(self, index: str , doc_id: str) -> dict:
        response = self.es.get(index, id=doc_id)
        return response['_source'] if response['found'] else None

    def update(self, index: str, doc_id: str, doc) -> bool:
        response = self.es.update(index, id=doc_id, body={'doc': doc})
        return response['result'] == 'updated'

    def delete(self, index: str, doc_id: str) -> bool:
        response = self.es.delete(index, id=doc_id)
        return response['result'] == 'deleted'

    def search(self, index: str, query: dict) -> list:
        response = self.es.search(index=index, body=query)
        return response['hits']['hits']


ES_INDEX = 'livros'


if __name__ == '__main__':
    document = {
        'nome': "Do Mil ao Milhão. Sem Cortar o Cafezinho",
        'valor': 25.50,
        'autor': "Thiago Nigro",
        'lancamento_dt': '2018-11-10',
        'tags': [
            "finanças", "investimentos", "dinheiro", "empreendedorismo"
        ]
    }

    es = ElasticSearchService()

    es.create_index(ES_INDEX)

    document_id = str(uuid4())
    es.create(ES_INDEX, document_id, doc=document)

    print(es.get(ES_INDEX, document_id))

    print(es.update(ES_INDEX, document_id, {"valor": 222222}))

    print(es.search(ES_INDEX, {'query': {'match_all': {}}}))

    print(es.delete(ES_INDEX, document_id))

    print(es.delete_index(ES_INDEX))

    print('main')

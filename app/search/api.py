import os
from django.http import JsonResponse
from elasticsearch import Elasticsearch

ES = Elasticsearch(os.getenv("ELASTICSEARCH_URL", "http://elastic:9200"))

def search_items(request):
    q = request.GET.get("q","")
    r = ES.search(
        index="items",
        query={"multi_match": {"query": q, "fields": ["descripcion^2","codigo^3","categoria_marca","categoria_linea"]}},
        size=20,
    )
    hits = r.get("hits", {}).get("hits", [])
    return JsonResponse({
        "took": r.get("took", 0),
        "total": r.get("hits", {}).get("total", {}).get("value", 0),
        "items": [h.get("_source", {}) for h in hits],
    })

def search_clients(request):
    q = request.GET.get("q","")
    r = ES.search(
        index="clients",
        query={"multi_match": {"query": q, "fields": ["razon_social^2","ruc","tipo_cliente"]}},
        size=20,
    )
    hits = r.get("hits", {}).get("hits", [])
    return JsonResponse({
        "took": r.get("took", 0),
        "total": r.get("hits", {}).get("total", {}).get("value", 0),
        "clients": [h.get("_source", {}) for h in hits],
    })

def suggest_items(request):
    q = request.GET.get("q","")
    r = ES.search(index="items", suggest={"s1": {"prefix": q, "completion": {"field": "suggest"}}}, size=0)
    return JsonResponse(r.get("suggest", {}).get("s1", [{}])[0])

def suggest_clients(request):
    q = request.GET.get("q","")
    r = ES.search(index="clients", suggest={"s1": {"prefix": q, "completion": {"field": "suggest"}}}, size=0)
    return JsonResponse(r.get("suggest", {}).get("s1", [{}])[0])

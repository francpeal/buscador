import os, json
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.db import connection
from elasticsearch import Elasticsearch

ES = Elasticsearch(os.getenv("ELASTICSEARCH_URL","http://elastic:9200"))

# -------- helpers
def _int(v, dflt):
    try:
        return int(v)
    except Exception:
        return dflt

def _list_param(request, name):
    # acepta ?marca=ACME&marca=XYZ o ?marca=ACME,XYZ
    vals = request.GET.getlist(name) or []
    if len(vals) == 1 and "," in vals[0]:
        vals = [x.strip() for x in vals[0].split(",") if x.strip()]
    return vals

# -------- ITEMS SEARCH
@require_GET
def search_items(request):
    q = (request.GET.get("q") or "").strip()
    page = max(_int(request.GET.get("page", 1), 1), 1)
    size = min(max(_int(request.GET.get("size", 20), 1), 1), 100)
    from_ = (page - 1) * size

    # filtros
    stock_min = _int(request.GET.get("stock_min", 0), 0)
    almacenes  = _list_param(request, "almacen")
    marcas     = _list_param(request, "marca")
    division   = request.GET.get("division")
    linea      = request.GET.get("linea")
    clase      = request.GET.get("clase")
    subclase   = request.GET.get("subclase")
    familia    = request.GET.get("familia")

    # sort
    sort_by = (request.GET.get("sort") or "relevance").lower()
    sort = []
    if sort_by == "stock":
        sort = [{"stock_total":"desc"}, "_score"]
    elif sort_by in ("ventas6m","ventas_6m"):
        sort = [{"venta_usd_6m":"desc"}, "_score"]
    elif sort_by in ("ultima","fecha","ultima_venta","fecha_ultima_venta"):
        sort = [{"fecha_ultima_venta":"desc"}, "_score"]
    # por defecto relevance (no agregamos sort)

    must = []
    if q:
        must.append({
            "multi_match": {
                "query": q,
                "fields": [
                    "descripcion^2",
                    "codigo^3",
                    "categoria_marca",
                    "categoria_linea",
                    "categoria_division",
                    "categoria_familia"
                ],
                "type": "best_fields",
                "operator": "and"
            }
        })
    else:
        must.append({"match_all": {}})

    filters = []
    if stock_min > 0:
        filters.append({"range": {"stock_total": {"gte": stock_min}}})

    # filtros exactos por término
    if division: filters.append({"term": {"categoria_division": division}})
    if linea:    filters.append({"term": {"categoria_linea": linea}})
    if clase:    filters.append({"term": {"categoria_clase": clase}})
    if subclase: filters.append({"term": {"categoria_subclase": subclase}})
    if familia:  filters.append({"term": {"categoria_familia": familia}})
    if marcas:
        filters.append({"terms": {"categoria_marca": marcas}})

    # filtro por almacén (nested)
    if almacenes:
        filters.append({
            "nested": {
                "path": "stock_por_almacen",
                "query": {
                    "bool": {
                        "must": [
                            {"terms": {"stock_por_almacen.almacen": almacenes}},
                            {"range": {"stock_por_almacen.qty": {"gt": 0}}}
                        ]
                    }
                }
            }
        })

    query = {"bool": {"must": must, "filter": filters}}

    body = {
        "from": from_,
        "size": size,
        "query": query,
        "highlight": {
            "pre_tags":["<mark>"], "post_tags":["</mark>"],
            "fields": {"descripcion": {"fragment_size": 120, "number_of_fragments": 1}}
        }
    }
    if sort: body["sort"] = sort

    r = ES.search(index="items", **body)
    hits = r.get("hits",{}).get("hits",[])
    items = []
    for h in hits:
        src = h.get("_source",{}).copy()
        hl = h.get("highlight",{})
        if "descripcion" in hl:
            src["descripcion_hl"] = hl["descripcion"][0]
        items.append(src)

    return JsonResponse({
        "page": page, "size": size,
        "took": r.get("took",0),
        "total": r.get("hits",{}).get("total",{}).get("value",0),
        "items": items
    })

# -------- CLIENTS SEARCH
@require_GET
def search_clients(request):
    q = (request.GET.get("q") or "").strip()
    page = max(_int(request.GET.get("page", 1), 1), 1)
    size = min(max(_int(request.GET.get("size", 20), 1), 1), 100)
    from_ = (page - 1) * size

    tipo = request.GET.get("tipo")  # tipo_cliente
    ruc  = request.GET.get("ruc")

    must = [{"match_all": {}}]
    if q:
        must = [{
            "multi_match": {
                "query": q,
                "fields": ["razon_social^2", "ruc", "tipo_cliente"],
                "type": "best_fields",
                "operator": "and"
            }
        }]

    filters = []
    if tipo: filters.append({"term": {"tipo_cliente": tipo}})
    if ruc:  filters.append({"term": {"ruc": ruc}})

    body = {
        "from": from_,
        "size": size,
        "query": {"bool": {"must": must, "filter": filters}},
        "sort": [{"venta_usd_6m":"desc"}, "_score"]
    }

    r = ES.search(index="clients", **body)
    hits = r.get("hits",{}).get("hits",[])
    return JsonResponse({
        "page": page, "size": size,
        "took": r.get("took",0),
        "total": r.get("hits",{}).get("total",{}).get("value",0),
        "clients": [h.get("_source",{}) for h in hits]
    })

# -------- DETALLE: Artículo y Cliente con histórico 6M (desde Postgres)
@require_GET
def item_detail(request, codigo):
    with connection.cursor() as cur:
        cur.execute("""
            SELECT codigo, descripcion, categoria_division, categoria_linea,
                   categoria_clase, categoria_subclase, categoria_familia, categoria_marca,
                   stock_total, stock_por_almacen, qty_6m, venta_usd_6m, fecha_ultima_venta
            FROM vw_items_search
            WHERE codigo = %s
        """, [codigo])
        row = cur.fetchone()
        if not row:
            return JsonResponse({"error":"not_found"}, status=404)
        cols = [c[0] for c in cur.description]
        item = dict(zip(cols, row))
        # Histórico mensual 6m
        cur.execute("""
            SELECT to_char(date_trunc('month', fecha), 'YYYY-MM') AS mes,
                   SUM(qty) AS qty, SUM(total_usd) AS venta
            FROM ventas
            WHERE codigo = %s AND fecha >= (CURRENT_DATE - INTERVAL '6 months')
            GROUP BY 1 ORDER BY 1
        """, [codigo])
        hist = [{"mes": m, "qty": int(q or 0), "venta": float(v or 0)} for (m,q,v) in cur.fetchall()]
    return JsonResponse({"item": item, "historico_6m": hist})

@require_GET
def client_detail(request, cliente_id):
    with connection.cursor() as cur:
        cur.execute("""
            SELECT cliente_id, ruc, razon_social, tipo_cliente,
                   qty_6m, venta_usd_6m, fecha_ultima_venta, productos_top_6m
            FROM vw_clients_search
            WHERE cliente_id = %s
        """, [cliente_id])
        row = cur.fetchone()
        if not row:
            return JsonResponse({"error":"not_found"}, status=404)
        cols = [c[0] for c in cur.description]
        cli = dict(zip(cols, row))
        # Histórico mensual 6m
        cur.execute("""
            SELECT to_char(date_trunc('month', fecha), 'YYYY-MM') AS mes,
                   SUM(qty) AS qty, SUM(total_usd) AS venta
            FROM ventas
            WHERE cliente_id = %s AND fecha >= (CURRENT_DATE - INTERVAL '6 months')
            GROUP BY 1 ORDER BY 1
        """, [cliente_id])
        hist = [{"mes": m, "qty": int(q or 0), "venta": float(v or 0)} for (m,q,v) in cur.fetchall()]
    return JsonResponse({"cliente": cli, "historico_6m": hist})

# -------- SUGGEST (se mantiene simple)
@require_GET
def suggest_items(request):
    q = (request.GET.get("q") or "").strip()
    r = ES.search(index="items", suggest={"s1":{"prefix":q, "completion":{"field":"suggest"}}}, size=0)
    return JsonResponse(r.get("suggest",{}).get("s1",[{}])[0])

@require_GET
def suggest_clients(request):
    q = (request.GET.get("q") or "").strip()
    r = ES.search(index="clients", suggest={"s1":{"prefix":q, "completion":{"field":"suggest"}}}, size=0)
    return JsonResponse(r.get("suggest",{}).get("s1",[{}])[0])

import os, json
from decimal import Decimal
from django.core.management.base import BaseCommand
from django.db import connection
from elasticsearch import Elasticsearch

ES = Elasticsearch(os.getenv("ELASTICSEARCH_URL","http://elastic:9200"))

ITEMS_MAPPING = {
  "settings":{"analysis":{"analyzer":{"es_text":{"tokenizer":"standard","filter":["lowercase","asciifolding"]}}}},
  "mappings":{"properties":{
    "codigo":{"type":"keyword"},
    "descripcion":{"type":"text","analyzer":"es_text"},
    "categoria_division":{"type":"keyword"},
    "categoria_linea":{"type":"keyword"},
    "categoria_clase":{"type":"keyword"},
    "categoria_subclase":{"type":"keyword"},
    "categoria_familia":{"type":"keyword"},
    "categoria_marca":{"type":"keyword"},
    "stock_total":{"type":"integer"},
    "stock_por_almacen":{"type":"nested","properties":{"almacen":{"type":"keyword"},"qty":{"type":"integer"}}},
    "qty_6m":{"type":"integer"},
    "venta_usd_6m":{"type":"scaled_float","scaling_factor":100},
    "fecha_ultima_venta":{"type":"date"},
    "suggest":{"type":"completion"}
  }}
}

CLIENTS_MAPPING = {
  "mappings":{"properties":{
    "cliente_id":{"type":"keyword"},
    "ruc":{"type":"keyword"},
    "razon_social":{"type":"text"},
    "tipo_cliente":{"type":"keyword"},
    "productos_top_6m":{"type":"keyword"},
    "qty_6m":{"type":"integer"},
    "venta_usd_6m":{"type":"scaled_float","scaling_factor":100},
    "fecha_ultima_venta":{"type":"date"},
    "suggest":{"type":"completion"}
  }}
}

def recreate_index(name, mapping):
    if ES.indices.exists(index=name): ES.indices.delete(index=name)
    ES.indices.create(index=name, body=mapping)

def rows(sql):
    with connection.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            yield dict(zip(cols, r))

class Command(BaseCommand):
    help = "Reindexa items/clients desde vistas Postgres"

    def add_arguments(self, p):
        p.add_argument("--target", choices=["items","clients","all"], default="all")

    def handle(self, *a, **o):
        tgt = o["target"]

        if tgt in ("items","all"):
            self.stdout.write("→ (re)creando índice items…")
            recreate_index("items", ITEMS_MAPPING)
            ops=[]
            for r in rows("SELECT * FROM vw_items_search"):
                # doc = {
                #     "codigo": r["codigo"], "descripcion": r["descripcion"],
                #     "categoria_division": r["categoria_division"], "categoria_linea": r["categoria_linea"],
                #     "categoria_clase": r["categoria_clase"], "categoria_subclase": r["categoria_subclase"],
                #     "categoria_familia": r["categoria_familia"], "categoria_marca": r["categoria_marca"],
                #     "stock_total": r["stock_total"], "stock_por_almacen": r["stock_por_almacen"],
                #     "qty_6m": r["qty_6m"], "venta_usd_6m": r["venta_usd_6m"],
                #     "fecha_ultima_venta": r["fecha_ultima_venta"],
                #     "suggest": {"input":[r["codigo"], r["descripcion"]]}
                # }
                # ops.extend([{"index":{"_index":"items","_id":r["codigo"]}}, doc])
                # if len(ops) >= 2000: ES.bulk(operations=ops); ops.clear()
                spa = r["stock_por_almacen"]
                if spa in (None, "", "null"):
                    spa = []
                spa = to_plain(spa)  # asegura lista de objetos [{almacen, qty}]
                doc = {
                    "codigo": r["codigo"],
                    "descripcion": r["descripcion"],
                    "categoria_division": r["categoria_division"],
                    "categoria_linea": r["categoria_linea"],
                    "categoria_clase": r["categoria_clase"],
                    "categoria_subclase": r["categoria_subclase"],
                    "categoria_familia": r["categoria_familia"],
                    "categoria_marca": r["categoria_marca"],
                    "stock_total": int(r["stock_total"] or 0),
                    "stock_por_almacen": spa,
                    "qty_6m": int(r["qty_6m"] or 0),
                    "venta_usd_6m": to_plain(r["venta_usd_6m"] or 0),
                    "fecha_ultima_venta": r["fecha_ultima_venta"],  # ES acepta ISO si viene date/datetime
                    "suggest": {"input":[r["codigo"], r["descripcion"]]}
                }
                ops.extend([{"index":{"_index":"items","_id":r["codigo"]}}, doc])
                if len(ops) >= 2000:
                    bulk_send(ops, label="items")
                    ops.clear()
            # if ops: ES.bulk(operations=ops)
            bulk_send(ops, label="items")

        if tgt in ("clients","all"):
            self.stdout.write("→ (re)creando índice clients…")
            recreate_index("clients", CLIENTS_MAPPING)
            ops=[]
            for r in rows("SELECT * FROM vw_clients_search"):
                doc = {
                    "cliente_id": r["cliente_id"], "ruc": r["ruc"],
                    "razon_social": r["razon_social"], "tipo_cliente": r["tipo_cliente"],
                    "productos_top_6m": r["productos_top_6m"],
                    "qty_6m": r["qty_6m"], "venta_usd_6m": r["venta_usd_6m"],
                    "fecha_ultima_venta": r["fecha_ultima_venta"],
                    "suggest": {"input":[r["ruc"], r["razon_social"]]}
                }
                ops.extend([{"index":{"_index":"clients","_id":r["cliente_id"]}}, doc])
                if len(ops) >= 2000: ES.bulk(operations=ops); ops.clear()
            if ops: ES.bulk(operations=ops)

        self.stdout.write(self.style.SUCCESS("Reindex completado."))


def to_plain(x):
    """Convierte Decimal/bytes/objetos a tipos JSON serializables."""
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, (bytes, memoryview)):
        x = bytes(x).decode("utf-8")
        try:
            return json.loads(x)
        except Exception:
            return x
    if isinstance(x, str):
        # si viene JSON en texto, parsea
        try:
            return json.loads(x)
        except Exception:
            return x
    if isinstance(x, list):
        return [to_plain(v) for v in x]
    if isinstance(x, dict):
        return {k: to_plain(v) for k, v in x.items()}
    return x

def bulk_send(ops, label="bulk"):
    if not ops:
        return
    resp = ES.bulk(operations=ops, refresh=False)
    if resp.get("errors"):
        # imprime el primer error representativo y falla
        for it in resp.get("items", []):
            err = it.get("index", {}).get("error")
            if err:
                raise RuntimeError(f"[{label}] Bulk error: {err}")
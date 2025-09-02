import os, asyncpg
from fastapi import FastAPI, Request, HTTPException

DB_URL = os.getenv("SUPABASE_DB_URL")  # postgres://USER:PASS@HOST:5432/postgres
RUN_TOKEN = os.getenv("RUN_TOKEN")     # any long random string

app = FastAPI()
pool: asyncpg.Pool | None = None

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(dsn=DB_URL, min_size=1, max_size=5)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

@app.get("/health")
async def health():
    return {"ok": True}

def ship_estimate_cents(weight_kg=None):
    if weight_kg is None: return 1200
    if weight_kg < 0.5: return 600
    if weight_kg < 2: return 1200
    return 2000

def valuate(price, med, sold30, var):
    if not med or not sold30:
        return ("PASS", 0, 0.0, 0.2, "Insufficient comps")
    fees = int(med * 0.13)
    ship = ship_estimate_cents()
    net = med - fees - ship - price
    margin = round(100 * (med - price) / max(price,1), 1)
    conf = round(0.6 * min(sold30/30,1.0) + 0.4 * (1 - min((var or 0)/0.25,1.0)), 2)
    if net >= 2000 and margin >= 25:
        return ("BUYABLE", net, margin, conf, ">$20 net & ≥25% margin")
    if net >= 1000 and margin >= 15:
        return ("WATCH", net, margin, conf, ">$10 net & ≥15% margin")
    return ("PASS", net, margin, conf, "Below thresholds")

async def fetch_candidates(conn: asyncpg.Connection):
    return await conn.fetch("""
      with latest_comp as (
        select distinct on (product_id) *
        from market_comps
        order by product_id, created_at desc
      ),
      latest_price as (
        select distinct on (product_id) *
        from price_snapshots
        order by product_id, created_at desc
      )
      select p.id as product_id, ps.id as price_id, mc.id as comp_id,
             ps.price_cents, mc.median_sale_cents, mc.sales_30d, mc.variance
      from products p
      join latest_price ps on ps.product_id = p.id
      left join latest_comp mc on mc.product_id = p.id
      where ps.created_at > now() - interval '2 hours'
    """)

@app.post("/run")
async def run(req: Request):
    # simple bearer token check
    auth = req.headers.get("authorization", "")
    if not RUN_TOKEN or auth != f"Bearer {RUN_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")

    assert pool is not None, "DB pool not ready"
    async with pool.acquire() as conn:
        rows = await fetch_candidates(conn)
        inserted = 0
        async with conn.transaction():
            for r in rows:
                status, net_cents, margin, conf, rationale = valuate(
                    r["price_cents"], r["median_sale_cents"], r["sales_30d"], r["variance"]
                )
                await conn.execute("""
                  insert into signals (product_id, price_snapshot_id, comp_id, status,
                                       net_profit_cents, margin_pct, confidence, rationale)
                  values ($1,$2,$3,$4,$5,$6,$7,$8)
                """, r["product_id"], r["price_id"], r["comp_id"],
                     status, net_cents, margin, conf, rationale)
                inserted += 1
    return {"processed": len(rows), "inserted": inserted}
import os
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

app = FastAPI(title="MaCasa API")

# Allow requests from GitHub Pages
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict to https://samsproducts123-afk.github.io in prod if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.environ.get("DATABASE_URL", "")
# Strip channel_binding param (not supported by asyncpg)
if "channel_binding" in DB_URL:
    import re
    DB_URL = re.sub(r'[&?]channel_binding=[^&]*', '', DB_URL)

@app.on_event("startup")
async def startup():
    if DB_URL:
        try:
            app.state.pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=10, ssl="require")
            print(f"✅ Database connected")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
    else:
        print("WARNING: DATABASE_URL not set")

@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, "pool"):
        await app.state.pool.close()

@app.get("/")
async def root():
    return {"status": "ok", "app": "MaCasa API"}

@app.get("/listings/bounds")
async def get_listings_by_bounds(
    nLat: float, sLat: float, eLng: float, wLng: float,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    min_surface: Optional[int] = None,
    min_rooms: Optional[int] = None,
    type_local: Optional[str] = None,
    limit: int = Query(50000, le=100000)
):
    """Get compact listings within map bounds"""
    if not hasattr(app.state, "pool"):
        raise HTTPException(500, "Database not connected")
        
    query = """
        SELECT lat, lng, price, surface, rooms, type, dpe, id
        FROM listings
        WHERE lat BETWEEN $1 AND $2 
          AND lng BETWEEN $3 AND $4
    """
    args = [sLat, nLat, wLng, eLng]
    
    # Optional filters
    idx = 5
    if min_price is not None:
        query += f" AND price >= ${idx}"
        args.append(min_price)
        idx += 1
    if max_price is not None:
        query += f" AND price <= ${idx}"
        args.append(max_price)
        idx += 1
    if min_surface is not None:
        query += f" AND surface >= ${idx}"
        args.append(min_surface)
        idx += 1
    if min_rooms is not None:
        query += f" AND rooms >= ${idx}"
        args.append(min_rooms)
        idx += 1
    if type_local:
        query += f" AND type = ${idx}"
        args.append(type_local)
        idx += 1
        
    query += f" LIMIT ${idx}"
    args.append(limit)
    
    async with app.state.pool.acquire() as conn:
        records = await conn.fetch(query, *args)
        
    # Ultra-compact response format matching LIVE_MAP structure
    # [lat*1e4, lon*1e4, price/100, surface, rooms, type(1/2), dpeIdx, id]
    DPE_MAP = {'A':1,'B':2,'C':3,'D':4,'E':5,'F':6,'G':7}
    
    compact = []
    for r in records:
        t = 1 if r['type'] == 'Appartement' else 2
        dpe = DPE_MAP.get(r['dpe'], 0)
        # Using lists saves massive JSON overhead
        compact.append([
            round(r['lat'] * 10000),
            round(r['lng'] * 10000),
            round(r['price'] / 100),
            r['surface'],
            r['rooms'],
            t,
            dpe,
            r['id']
        ])
        
    return {"count": len(compact), "data": compact}

@app.get("/listings/{listing_id}")
async def get_listing(listing_id: str):
    """Get full details for a single listing"""
    if not hasattr(app.state, "pool"):
        raise HTTPException(500, "Database not connected")
        
    async with app.state.pool.acquire() as conn:
        record = await conn.fetchrow("SELECT * FROM listings WHERE id = $1", listing_id)
        
    if not record:
        raise HTTPException(404, "Listing not found")
        
    return dict(record)

@app.get("/stats")
async def get_stats():
    """Get database stats"""
    if not hasattr(app.state, "pool"):
        raise HTTPException(500, "Database not connected")
        
    async with app.state.pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM listings")
        with_price = await conn.fetchval("SELECT count(*) FROM listings WHERE price > 0")
        
    return {"total_listings": count, "with_price": with_price}

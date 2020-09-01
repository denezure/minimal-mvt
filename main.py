from flask import Flask, make_response, g
import psycopg2

app = Flask(__name__)

DATABASE = {
    'user':     'pramsey',
    'password': 'password',
    'host':     'localhost',
    'port':     '5432',
    'database': 'nyc'
}


@app.route('/tile/<string:table>/<int:zoom>/<int:x>/<int:y>/<string:format>')
def get_tile(table, zoom, x, y, format):
    # Validate tile
    size = 2 ** zoom
    if x >= size or y >= size or x < 0 or y < 0:
        return ('invalid request', 400)

    # TODO: Validate the table name

    # Calculate envelope in spherical mercator
    # Width of world in EPSG:3857
    worldMercMax = 20037508.3427892
    worldMercMin = -1 * worldMercMax
    worldMercSize = worldMercMax - worldMercMin
    # Width in tiles
    worldTileSize = 2 ** size
    # Tile width in EPSG:3857
    tileMercSize = worldMercSize / worldTileSize

    xmin = worldMercMin + tileMercSize * x
    xmax = worldMercMin + tileMercSize * (x + 1)
    ymin = worldMercMax - tileMercSize * (y + 1)
    ymax = worldMercMax - tileMercSize * y

    # Generate SQL to materialize a query envelope in EPSG:3857
    # Densify the edges a little so the envelope can be
    # safely converted to other coordinate systems.
    DENSIFY_FACTOR = 4
    segSize = (xmax - xmin)/DENSIFY_FACTOR
    # No idea what this magic # is
    magic1 = 3857
    env = f'ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857),{segSize})'

    # Generate a SQL query to pull a tile worth of MVT data
    # from the table of interest.

    # Magic string ?
    geomColumn = 'geom'
    # No idea what this magic # is
    srid = 26918

    sql_query = f'''
        WITH 
            bounds AS (
                SELECT {env} AS geom, 
                       {env}::box2d AS b2d
            ),
            mvtgeom AS (
                SELECT ST_AsMVTGeom(ST_Transform(t.{geomColumn}, {magic1}), bounds.b2d) AS geom, gid, name, type
                FROM {table} t, bounds
                WHERE ST_Intersects(t.{geomColumn}, ST_Transform(bounds.geom, {srid}))
            ) 
            SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom
    '''

    # TODO: Exception handling
    if 'db' not in g:
        g.db = pyscopg2.connect(**DATABASE)

    db_conn = g.db

    with db_conn.cursor() as cursor:
        cursor.execute(sql_query)
        if not cursor:
            return ('db request failed', 400)

        resp = make_response(cursor.fetchone()[0])
        resp.headers['Content-type'] = 'application/vnd.mapbox-vector-tile'
        resp.headers['Access-Control-Allow-Origin'] = '*'

        return resp

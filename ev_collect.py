#!/usr/bin/env python3
"""
ev_collect.py — collect EV charging point availability from LTA DataMall.

COR1305 team project. Standard library only, so there is nothing to install.

    python3 ev_collect.py probe                 look once, see what comes back
    python3 ev_collect.py collect               run forever, poll every 15 min
    python3 ev_collect.py collect --every 30    ... every 30 minutes instead
    python3 ev_collect.py aggregate             turn the log into hourly rows

Put your LTA DataMall **API** Account Key (not the SDK one) in a file called
lta_key.txt next to this script, or set the LTA_KEY environment variable.

How the API works (LTA DataMall API User Guide v6.9, section 2.29):
  1. GET .../EVCBatch with an AccountKey header. It does NOT return the data.
     It returns a Link: a temporary download URL that expires in 15 minutes.
  2. GET that Link. That is the actual JSON file with every charger in it.
The feed refreshes every 5 minutes, so polling faster only gives duplicates.

Shape of the batch file (confirmed against the live feed, Sep 2026):
  { "LastUpdatedTime": "...",
    "evLocationsData": [
      { address, name, longtitude, latitude, postalCode,
        "chargingPoints": [
          { status, operatingHours, operator, position, name,
            "plugTypes": [
              { plugType, price, current, powerRating, priceType,
                "evIds": [ { evCpId, status } ] } ] } ] } ] }

Note this differs from the per-postal-code endpoint documented in the guide:
evIds is nested INSIDE plugTypes, there is no locationId, and 'powerRating'
holds the kW figure while AC/DC lives in 'current'.
"""

import argparse
import csv
import gzip
import json
import os
import signal
import ssl
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

BATCH_URL = 'https://datamall2.mytransport.sg/ltaodataservice/EVCBatch'
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(HERE, 'ev_raw.csv')
LOG_PATH = os.path.join(HERE, 'ev_collect.log')
STATE_PATH = os.path.join(HERE, '.ev_last_feed')

COLUMNS = [
    'ts_local', 'ts_epoch', 'date', 'hour', 'feed_updated',
    'station_key', 'postal_code', 'station_name', 'address', 'latitude', 'longitude',
    'operator', 'position', 'operating_hours', 'charger_status',
    'plug_type', 'current', 'power_rating_kw', 'price', 'price_type',
    'ev_cp_id', 'connector_status', 'status_label',
]

STATUS_LABEL = {'0': 'occupied', '1': 'available', '': 'out_of_service'}

_stop = False


def log(msg):
    line = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  {msg}'
    print(line, flush=True)
    try:
        with open(LOG_PATH, 'a', encoding='utf-8') as fh:
            fh.write(line + '\n')
    except OSError:
        pass


def read_key():
    key = os.environ.get('LTA_KEY', '').strip()
    if key:
        return key
    path = os.path.join(HERE, 'lta_key.txt')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as fh:
            key = fh.read().strip().strip('"').strip("'").strip()
        if key:
            return key
    sys.exit('No API key found. Put your DataMall **API** Account Key in '
             'lta_key.txt next to this script, or set LTA_KEY.\n'
             'Note: the SDK Account Key is a different key and will not work.')


CERT_HELP = """
--------------------------------------------------------------------------
Python cannot verify HTTPS certificates on this machine.

This is a known macOS quirk, not a problem with your API key or this script.
Python installed from python.org does not wire up its CA certificates.

FIX (run once, then try again):

    /Applications/Python\\ 3.x/Install\\ Certificates.command

replacing 3.x with your version -- look in Applications for a "Python 3.x"
folder and double-click "Install Certificates.command" inside it.

If that file is missing, this works instead:

    python3 -m pip install --upgrade certifi

then run this script again -- it will pick certifi up automatically.
--------------------------------------------------------------------------"""


def _ssl_context():
    """Use certifi's CA bundle when available; fall back to the default."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


_CTX = _ssl_context()


def http_get(url, headers=None, timeout=90):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as resp:
            raw = resp.read()
    except urllib.error.URLError as e:
        if isinstance(getattr(e, 'reason', None), ssl.SSLCertVerificationError):
            raise RuntimeError('SSL certificate verification failed.' + CERT_HELP) from None
        raise
    if raw[:2] == b'\x1f\x8b':                       # gzipped payload
        raw = gzip.decompress(raw)
    return raw


def fetch_batch(key, timeout=90):
    """Two-step fetch. Returns the parsed JSON of the batch file."""
    meta_raw = http_get(BATCH_URL, {'AccountKey': key, 'accept': 'application/json'},
                        timeout=timeout)
    meta = json.loads(meta_raw.decode('utf-8', 'replace'))
    link = find_link(meta)
    if not link:
        raise RuntimeError('No download Link in the EVCBatch response. '
                           f'Got keys: {list(meta)[:8]}')
    return json.loads(http_get(link, timeout=timeout).decode('utf-8', 'replace'))


def find_link(obj):
    """The Link sits inside a 'value' list, but look anywhere for robustness."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() == 'link' and isinstance(v, str) and v.startswith('http'):
                return v
        for v in obj.values():
            found = find_link(v)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = find_link(v)
            if found:
                return found
    return None


def find_stations(obj):
    """'evLocationsData' in the live feed; fall back to a generic search."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() == 'evlocationsdata' and isinstance(v, list):
                return v
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and _looks_like_station(obj[0]):
            return obj
        for v in obj:
            found = find_stations(v)
            if found:
                return found
    elif isinstance(obj, dict):
        for v in obj.values():
            found = find_stations(v)
            if found:
                return found
    return None


def _looks_like_station(d):
    keys = {k.lower() for k in d}
    return 'chargingpoints' in keys


def g(d, *names, default=''):
    """Case-insensitive get, tolerant of LTA's 'longtitude' typo."""
    if not isinstance(d, dict):
        return default
    low = {k.lower(): v for k, v in d.items()}
    for n in names:
        v = low.get(n.lower())
        if v not in (None, ''):
            return v
    return default


def flatten(stations, now, feed_updated=''):
    """One row per connector — that is the level utilisation lives at."""
    ts_local = now.astimezone().isoformat(timespec='seconds')
    local = now.astimezone()
    rows = []
    for st in stations:
        if not isinstance(st, dict):
            continue
        postal = str(g(st, 'postalCode'))
        sname = g(st, 'name')
        base = {
            'ts_local': ts_local,
            'ts_epoch': int(now.timestamp()),
            'date': local.strftime('%Y-%m-%d'),
            'hour': local.hour,
            'feed_updated': feed_updated,
            'station_key': f'{postal}|{sname}'.strip('|'),
            'postal_code': postal,
            'station_name': sname,
            'address': g(st, 'address'),
            'latitude': g(st, 'latitude'),
            'longitude': g(st, 'longtitude', 'longitude'),   # LTA spells it wrong
        }
        for cp in g(st, 'chargingPoints', default=[]) or []:
            if not isinstance(cp, dict):
                continue
            cbase = dict(base)
            cbase.update({
                'operator': g(cp, 'operator'),
                'position': g(cp, 'position'),
                'operating_hours': g(cp, 'operatingHours', 'operationHours'),
                'charger_status': str(g(cp, 'status')),
            })
            plugs = g(cp, 'plugTypes', default=[]) or []
            for plug in plugs:
                if not isinstance(plug, dict):
                    continue
                pbase = dict(cbase)
                pbase.update({
                    'plug_type': g(plug, 'plugType'),
                    'current': g(plug, 'current'),              # AC or DC
                    'power_rating_kw': g(plug, 'powerRating'),  # kW, despite the name
                    'price': g(plug, 'price'),
                    'price_type': g(plug, 'priceType'),
                })
                # evIds is nested inside plugTypes in the batch feed
                evids = g(plug, 'evIds', default=None)
                if evids is None:
                    evids = g(cp, 'evIds', default=[]) or []     # documented shape
                for ev in evids or []:
                    if not isinstance(ev, dict):
                        continue
                    status = g(ev, 'status', default='')
                    status = '' if status is None else str(status).strip()
                    row = dict(pbase)
                    row.update({
                        'ev_cp_id': g(ev, 'evCpId', 'id'),
                        'connector_status': status,
                        'status_label': STATUS_LABEL.get(status, 'out_of_service'),
                    })
                    rows.append(row)
    return rows


def load_scope(path):
    if not path:
        return None
    keep = set()
    with open(path, encoding='utf-8') as fh:
        for line in fh:
            v = line.strip().strip('"').strip()
            if v and not v.lower().startswith(('station_key', 'ev_cp_id',
                                               'postal_code', '#')):
                keep.add(v)
    log(f'scope filter loaded: {len(keep)} ids')
    return keep


def in_scope(row, keep):
    return (row['ev_cp_id'] in keep or row['station_key'] in keep
            or row['postal_code'] in keep)


def write_rows(path, rows):
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, 'a', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction='ignore')
        if new:
            w.writeheader()
        w.writerows(rows)
        fh.flush()
        os.fsync(fh.fileno())


def write_rows_gz(path, rows):
    """Append to a gzipped CSV. Gzip members concatenate, so this reads back fine."""
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    new = not os.path.exists(path)
    with gzip.open(path, 'at', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction='ignore')
        if new:
            w.writeheader()
        w.writerows(rows)


def summarise(rows):
    c = defaultdict(int)
    for r in rows:
        c[r['status_label']] += 1
    live = c['occupied'] + c['available']
    return c, (c['occupied'] / live if live else None)


# --------------------------------------------------------------------- probe
def cmd_probe(args):
    key = read_key()
    log('calling EVCBatch ...')
    try:
        meta_raw = http_get(BATCH_URL, {'AccountKey': key, 'accept': 'application/json'})
    except urllib.error.HTTPError as e:
        body = e.read()[:400].decode('utf-8', 'replace')
        sys.exit(f'\nHTTP {e.code} from EVCBatch.\n{body}\n\n'
                 '401 or 403 usually means you used the SDK Account Key instead of '
                 'the API Account Key, or the key is not active yet.')
    except RuntimeError as e:
        sys.exit(f'\n{e}')
    except urllib.error.URLError as e:
        sys.exit(f'\nCould not reach LTA DataMall: {e.reason}\nCheck the connection.')

    meta = json.loads(meta_raw.decode('utf-8', 'replace'))
    link = find_link(meta)
    if not link:
        sys.exit(f'\nNo Link in the response:\n{json.dumps(meta, indent=2)[:600]}')
    print('\n--- downloading the batch file (link expires in 15 min) ---')
    data = json.loads(http_get(link).decode('utf-8', 'replace'))

    sample = os.path.join(HERE, 'ev_sample.json')
    with open(sample, 'w', encoding='utf-8') as fh:
        json.dump(data, fh, indent=2)
    print(f'raw file saved to {sample}')

    stations = find_stations(data)
    if stations is None:
        sys.exit(f'Station list not found. Top level: {list(data)[:10]}')

    feed = g(data, 'LastUpdatedTime') if isinstance(data, dict) else ''
    rows = flatten(stations, datetime.now(timezone.utc), feed)
    print(f'\nfeed last updated  : {feed}')
    print(f'stations           : {len(stations):,}')
    print(f'connector rows     : {len(rows):,}')
    if not rows:
        sys.exit('Still zero rows — send ev_sample.json to Claude.')

    counts, occ = summarise(rows)
    print('\nstatus right now:')
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f'   {k:16} {v:7,}  ({v/len(rows):.1%})')
    if occ is not None:
        print(f'\n   occupancy = occupied / (occupied + available) = {occ:.1%}')

    ops = defaultdict(int)
    ac_dc = defaultdict(int)
    prices = defaultdict(list)
    for r in rows:
        ops[r['operator'] or '(blank)'] += 1
        ac_dc[r['current'] or '(blank)'] += 1
        try:
            prices[r['current']].append(float(r['price']))
        except (TypeError, ValueError):
            pass
    print('\ntop operators:')
    for k, v in sorted(ops.items(), key=lambda x: -x[1])[:10]:
        print(f'   {k:34} {v:6,}')
    print('\nAC / DC split:')
    for k, v in sorted(ac_dc.items(), key=lambda x: -x[1]):
        p = prices.get(k) or []
        avg = f'  avg ${sum(p)/len(p):.4f}/kWh' if p else ''
        print(f'   {k:10} {v:6,}{avg}')
    print('\nfirst row:')
    for k in COLUMNS:
        print(f'   {k:18} {rows[0][k]}')
    print('\nLooks right? Then run:  python3 ev_collect.py collect')


# ---------------------------------------------------------------------- once
def cmd_once(args):
    """A single poll. This is what GitHub Actions runs on a schedule."""
    key = read_key()
    scope = load_scope(args.scope)
    state = os.path.join(args.dir, '.last_feed')

    last_feed = ''
    if os.path.exists(state):
        try:
            last_feed = open(state, encoding='utf-8').read().strip()
        except OSError:
            pass

    data = fetch_batch(key)
    stations = find_stations(data)
    if stations is None:
        sys.exit('station list not found in batch file')
    feed = g(data, 'LastUpdatedTime') if isinstance(data, dict) else ''

    if feed and feed == last_feed and not args.keep_duplicates:
        log(f'feed unchanged ({feed}) — nothing written')
        return

    now = datetime.now(timezone.utc)
    rows = flatten(stations, now, feed)
    if scope:
        rows = [r for r in rows if in_scope(r, scope)]
    if not rows:
        log('no rows after filtering — check the scope file')
        return

    path = os.path.join(args.dir, f'ev_{now.astimezone().strftime("%Y-%m-%d")}.csv.gz')
    write_rows_gz(path, rows)
    os.makedirs(args.dir, exist_ok=True)
    with open(state, 'w', encoding='utf-8') as fh:
        fh.write(feed)
    _, occ = summarise(rows)
    rate = f'{occ:.1%}' if occ is not None else 'n/a'
    log(f'wrote {len(rows):,} rows to {path} (occupancy {rate}, feed {feed})')


# ------------------------------------------------------------------- collect
def cmd_collect(args):
    key = read_key()
    scope = load_scope(args.scope)
    interval = max(5, args.every) * 60
    log(f'collecting every {args.every} min -> {args.out}')
    log('Ctrl-C to stop. Safe to restart: it appends.')

    last_feed = ''
    if os.path.exists(STATE_PATH):
        try:
            last_feed = open(STATE_PATH, encoding='utf-8').read().strip()
        except OSError:
            pass

    cycles = failures = skipped = total = 0
    while not _stop:
        started = time.time()
        try:
            data = fetch_batch(key)
            stations = find_stations(data)
            if stations is None:
                raise RuntimeError('station list not found in batch file')
            feed = g(data, 'LastUpdatedTime') if isinstance(data, dict) else ''

            if feed and feed == last_feed and not args.keep_duplicates:
                skipped += 1
                log(f'feed unchanged ({feed}) — nothing new, skipped')
            else:
                rows = flatten(stations, datetime.now(timezone.utc), feed)
                if scope:
                    rows = [r for r in rows if in_scope(r, scope)]
                if rows:
                    write_rows(args.out, rows)
                    total += len(rows)
                    cycles += 1
                    _, occ = summarise(rows)
                    rate = f'{occ:.1%}' if occ is not None else 'n/a'
                    log(f'poll {cycles}: {len(rows):,} rows, occupancy {rate}, '
                        f'{total:,} rows total')
                    last_feed = feed
                    try:
                        with open(STATE_PATH, 'w', encoding='utf-8') as fh:
                            fh.write(feed)
                    except OSError:
                        pass
                else:
                    log('no rows after filtering — check the scope file')
        except KeyboardInterrupt:
            break
        except Exception as e:                        # never let the loop die
            failures += 1
            log(f'FAILED ({type(e).__name__}): {e}  [{failures} failures so far]')
            if failures % 10 == 0:
                log('   many failures — internet down? key expired?')

        slept = 0
        wait = max(1, interval - (time.time() - started))
        while slept < wait and not _stop:             # stay responsive to Ctrl-C
            time.sleep(min(2, wait - slept))
            slept += 2
    log(f'stopped. {cycles} polls written, {skipped} skipped as unchanged, '
        f'{failures} failures, {total:,} rows.')


# ----------------------------------------------------------------- aggregate
STATIC = ['station_key', 'postal_code', 'station_name', 'address', 'latitude',
          'longitude', 'operator', 'position', 'plug_type', 'current',
          'power_rating_kw', 'price', 'price_type']


def _iter_rows(target):
    """Accept a single .csv / .csv.gz, or a directory holding many of them."""
    import glob
    if os.path.isdir(target):
        paths = sorted(glob.glob(os.path.join(target, '*.csv.gz')) +
                       glob.glob(os.path.join(target, '*.csv')))
        if not paths:
            sys.exit(f'No .csv or .csv.gz files in {target}')
    else:
        paths = [target]
    for p in paths:
        opener = gzip.open if p.endswith('.gz') else open
        log(f'   reading {os.path.basename(p)}')
        with opener(p, 'rt', newline='', encoding='utf-8') as fh:
            for r in csv.DictReader(fh):
                if r.get('ev_cp_id') and r.get('ev_cp_id') != 'ev_cp_id':
                    yield r


def cmd_aggregate(args):
    if not os.path.exists(args.out):
        sys.exit(f'{args.out} not found — collect some data first.')
    log(f'aggregating {args.out} ...')
    buckets, n = {}, 0
    if True:
        for r in _iter_rows(args.out):
            n += 1
            k = (r['ev_cp_id'], r['date'], r['hour'])
            b = buckets.get(k)
            if b is None:
                b = buckets[k] = {s: r.get(s, '') for s in STATIC}
                b.update({'ev_cp_id': r['ev_cp_id'], 'date': r['date'],
                          'hour': r['hour'], 'polls': 0,
                          'occupied': 0, 'available': 0, 'out_of_service': 0})
            b['polls'] += 1
            b[r['status_label']] = b.get(r['status_label'], 0) + 1

    out = (os.path.join(args.out, 'hourly.csv') if os.path.isdir(args.out)
           else os.path.splitext(args.out)[0] + '_hourly.csv')
    cols = (['ev_cp_id', 'date', 'hour', 'weekday'] + STATIC +
            ['polls', 'occupied', 'available', 'out_of_service',
             'occupancy_rate', 'uptime_rate'])
    with open(out, 'w', newline='', encoding='utf-8') as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        for b in buckets.values():
            live = b['occupied'] + b['available']
            b['occupancy_rate'] = round(b['occupied'] / live, 4) if live else ''
            b['uptime_rate'] = round(live / b['polls'], 4) if b['polls'] else ''
            try:
                b['weekday'] = datetime.strptime(b['date'], '%Y-%m-%d').strftime('%a')
            except ValueError:
                b['weekday'] = ''
            w.writerow(b)
    log(f'read {n:,} raw rows -> wrote {len(buckets):,} connector-hours to {out}')
    log('that file is the one to load into the model.')


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='cmd', required=True)

    sub.add_parser('probe', help='one look at the feed, then stop')

    c = sub.add_parser('collect', help='poll on a loop and append to CSV')
    c.add_argument('--every', type=int, default=15, help='minutes between polls (min 5)')
    c.add_argument('--out', default=DEFAULT_OUT)
    c.add_argument('--scope', help='text file of ev_cp_id, station_key or postal_code to keep')
    c.add_argument('--keep-duplicates', action='store_true',
                   help='write even when the feed has not refreshed since last poll')

    o = sub.add_parser('once', help='one poll, then exit (for GitHub Actions / cron)')
    o.add_argument('--dir', default='data', help='folder for dated .csv.gz files')
    o.add_argument('--scope', help='text file of ev_cp_id, station_key or postal_code')
    o.add_argument('--keep-duplicates', action='store_true')

    a = sub.add_parser('aggregate', help='roll the log up to connector-hours')
    a.add_argument('--out', default=DEFAULT_OUT,
                   help='a .csv / .csv.gz file, or a folder of them')

    args = p.parse_args()
    {'probe': cmd_probe, 'once': cmd_once, 'collect': cmd_collect,
     'aggregate': cmd_aggregate}[args.cmd](args)


def _sigint(signum, frame):
    global _stop
    _stop = True
    log('stopping after this poll ...')


if __name__ == '__main__':
    signal.signal(signal.SIGINT, _sigint)
    main()

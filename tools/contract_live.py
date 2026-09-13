"""Contract check: a DEPLOYED API against every field the frontend reads.

WHY THIS EXISTS

The unit suite runs on SQLite and mocks nothing, which makes it fast and makes
it miss an entire class of bug. It passed 600 times while POST /social/posts
returned 500 on production for every driver, because the id from
`RETURNING id` was read positionally and the Postgres pool hands back
dict-like rows. No amount of SQLite testing finds that.

This script talks to a real deployment. It is deliberately NOT part of pytest:
it creates an account and a post on whatever it is pointed at, and a test
suite that writes to production on `pytest` is a trap.

RUNNING IT

    JOSEO_CONTRACT_URL=https://staging.example.com python3 tools/contract_live.py

Point it at staging if there is one. Pointing it at production is legitimate
after a deploy -- that is what it was written for -- but it leaves a throwaway
account behind, which it prints. The post and comment it creates are deleted
before it exits.

WHAT IT CHECKS

Not rendering. The shapes: every field feed.js, profile.js, compose.js and
map-action.js read, on real responses, plus the behaviours that only show up
against a real database -- that a patch leaves untouched fields alone, that a
zone tag round-trips, that a comment count agrees between the feed and the
single post.
"""
import json, time, sys, urllib.request, urllib.error, os

API = os.environ.get("JOSEO_CONTRACT_URL",
                     "https://web-production-78f67.up.railway.app").rstrip("/")
if not os.environ.get("JOSEO_CONTRACT_CONFIRM") and "railway.app" in API:
    print("Refusing to run against production without JOSEO_CONTRACT_CONFIRM=1.")
    print("This creates a real account and a real post. Point JOSEO_CONTRACT_URL")
    print("at a staging deployment, or set the variable if production is intended.")
    sys.exit(2)
STAMP = int(time.time())
EMAIL = f"joseo-contract-{STAMP}@example.com"
NAME = f"Contract Test {STAMP}"

proxy = os.environ.get("HTTPS_PROXY", "")
opener = urllib.request.build_opener(
    urllib.request.ProxyHandler({"https": proxy, "http": proxy}) if proxy
    else urllib.request.ProxyHandler({}))

results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("  ok   " if ok else "  FAIL ") + name + (f"\n         {detail}" if not ok else ""))


def call(method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with opener.open(req, timeout=45) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw[:200]}


def has(obj, fields, label):
    missing = [f for f in fields if f not in (obj or {})]
    check(f"{label} carries every field the UI reads", not missing, f"missing {missing}")
    return not missing


status, signup = call("POST", "/auth/signup", {
    "email": EMAIL, "password": "contract-test-password-123",
    "display_name": NAME, "city": "New York, NY"})
check("signup", status == 200, f"{status} {json.dumps(signup)[:200]}")
if status != 200:
    sys.exit(1)
token = signup["token"]
me_id = signup.get("id")

# ---- profile ------------------------------------------------------------
status, out = call("GET", "/social/me/profile", token=token)
check("GET /social/me/profile", status == 200, f"{status} {json.dumps(out)[:200]}")
p = out.get("profile", {})
has(p, ["user_id", "display_name", "handle", "city", "avatar_url", "bio",
        "platforms", "vehicle_type", "driving_since_year", "reputation",
        "post_count", "follower_count", "following_count", "followed_by_me",
        "is_me"], "profile")
check("the city sent at signup was stored", p.get("city") == "New York, NY",
      f"city={p.get('city')!r}")
check("following_count is served to its owner", p.get("following_count") is not None,
      f"following_count={p.get('following_count')!r}")
has(p.get("reputation") or {}, ["level", "rank_name", "title", "trips_logged",
                                "lifetime_miles"], "reputation")

# ---- closed sets --------------------------------------------------------
status, out = call("GET", "/social/identity/options", token=token)
check("GET /social/identity/options", status == 200, str(status))
check("the platform and vehicle sets are non-empty",
      len(out.get("platforms") or []) >= 5 and len(out.get("vehicle_types") or []) >= 5,
      json.dumps(out)[:200])
VEHICLES = out.get("vehicle_types") or []

# ---- identity patch semantics -------------------------------------------
status, out = call("POST", "/social/me/identity",
                   {"bio": "Contract test bio.", "platforms": ["uber", "lyft"]}, token)
check("POST /social/me/identity", status == 200, f"{status} {json.dumps(out)[:200]}")
p = out.get("profile", {})
check("the bio and platforms saved",
      p.get("bio") == "Contract test bio." and p.get("platforms") == ["uber", "lyft"],
      json.dumps({k: p.get(k) for k in ("bio", "platforms")}))

vehicle = VEHICLES[0] if VEHICLES else "sedan"
status, out = call("POST", "/social/me/identity", {"vehicle_type": vehicle}, token)
p = out.get("profile", {})
check("a patch leaves the fields it did not mention alone",
      p.get("bio") == "Contract test bio." and p.get("vehicle_type") == vehicle,
      json.dumps({k: p.get(k) for k in ("bio", "vehicle_type")}))

# ---- handle -------------------------------------------------------------
status, out = call("GET", f"/social/handles/contracttest{STAMP}/available", token=token)
check("GET /social/handles/{h}/available", status == 200 and "available" in out,
      f"{status} {json.dumps(out)[:200]}")

# ---- avatar ------------------------------------------------------------
# /avatars/thumb/{id} is served `immutable` for 30 days, so the URL MUST carry
# the ?v= or a driver who changes their picture shows the old one for a month.
# The social surfaces shipped without it once; this is the live guard.
import base64
_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000a49444154789c6300010000050001"
    "0d0a2db40000000049454e44ae426082")
status, _ = call("POST", "/me/update", {
    "avatar_url": "data:image/png;base64," + base64.b64encode(_PNG).decode()}, token)
check("POST /me/update with an avatar", status == 200, str(status))
status, out = call("GET", "/social/me/profile", token=token)
avatar = out.get("profile", {}).get("avatar_url")
check("the profile avatar url carries its version",
      bool(avatar) and "?v=" in str(avatar), f"avatar_url={avatar!r}")

# ---- post ---------------------------------------------------------------
status, out = call("POST", "/social/posts", {
    "body": "Automated contract test - please ignore.",
    "zone_name": "JFK Airport", "zone_rating": 91,
    "lat": 40.6413, "lng": -73.7781}, token)
check("POST /social/posts", status == 200, f"{status} {json.dumps(out)[:200]}")
post = out.get("post", {})
post_id = post.get("id")
has(post, ["id", "author", "body", "image_url", "image_thumb_url", "city",
           "zone_name", "zone_rating", "like_count", "liked_by_me",
           "comment_count", "mine", "created_at"], "post")
has(post.get("author") or {}, ["user_id", "display_name", "handle", "city",
                               "avatar_url", "level", "platforms"], "post author")
check("compose's zone tag round-trips",
      post.get("zone_name") == "JFK Airport" and post.get("zone_rating") == 91,
      json.dumps({k: post.get(k) for k in ("zone_name", "zone_rating")}))
check("the author's platforms reach the card",
      post.get("author", {}).get("platforms") == ["uber", "lyft"],
      json.dumps(post.get("author", {}).get("platforms")))
post_avatar = post.get("author", {}).get("avatar_url")
check("the card's avatar url carries its version",
      bool(post_avatar) and "?v=" in str(post_avatar), f"avatar_url={post_avatar!r}")

# ---- feed ---------------------------------------------------------------
for scope in ("following", "city", "everyone"):
    status, out = call("GET", f"/social/feed?scope={scope}&limit=5", token=token)
    check(f"GET /social/feed?scope={scope}", status == 200 and "items" in out,
          f"{status} {json.dumps(out)[:200]}")
    check(f"the {scope} feed answers with a cursor field", "next_before_id" in out,
          json.dumps(list(out))[:200])

status, out = call("GET", "/social/feed?scope=following&limit=5", token=token)
mine = [i for i in out.get("items", []) if i.get("id") == post_id]
check("the new post is in the driver's own following feed", len(mine) == 1,
      f"{len(out.get('items', []))} items")

# ---- like ---------------------------------------------------------------
status, out = call("POST", f"/social/posts/{post_id}/like", None, token)
check("POST like", status == 200 and out.get("like_count") == 1
      and out.get("liked_by_me") is True, f"{status} {json.dumps(out)[:200]}")
status, out = call("DELETE", f"/social/posts/{post_id}/like", None, token)
check("DELETE like", status == 200 and out.get("like_count") == 0,
      f"{status} {json.dumps(out)[:200]}")

# ---- comments -----------------------------------------------------------
status, out = call("POST", f"/social/posts/{post_id}/comments",
                   {"body": "Contract test reply."}, token)
check("POST comment", status == 200, f"{status} {json.dumps(out)[:200]}")
comment = out.get("comment", {})
comment_id = comment.get("id")
has(comment, ["id", "post_id", "author", "body", "mine", "can_delete",
              "created_at"], "comment")
check("the comment count comes back with the write", out.get("comment_count") == 1,
      json.dumps(out)[:200])
check("your own comment is deletable", comment.get("can_delete") is True,
      json.dumps(comment)[:200])

status, out = call("GET", f"/social/posts/{post_id}/comments?limit=20", token=token)
check("GET comments", status == 200 and len(out.get("items") or []) == 1,
      f"{status} {json.dumps(out)[:200]}")
check("the thread answers with a forward cursor field", "next_after_id" in out,
      json.dumps(list(out))[:200])

status, out = call("GET", f"/social/posts/{post_id}", token=token)
check("comment_count reaches the single post too",
      out.get("post", {}).get("comment_count") == 1, json.dumps(out)[:200])

status, out = call("DELETE", f"/social/comments/{comment_id}", None, token)
check("DELETE comment", status == 200 and out.get("comment_count") == 0,
      f"{status} {json.dumps(out)[:200]}")

# ---- cleanup ------------------------------------------------------------
status, _ = call("DELETE", f"/social/posts/{post_id}", None, token)
check("the contract-test post was deleted", status == 200, f"status {status}")

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} contract checks passed")
if failed:
    print("failed:")
    for f in failed:
        print("  -", f)
print(f"\nthrowaway account left behind: {EMAIL} (id {me_id})")
sys.exit(1 if failed else 0)

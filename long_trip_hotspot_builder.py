"""
NYC long-trip hotspot CLUSTERS — point pins on the map.

Different from zone-level overlays: this module produces a small list of
specific lat/lng points where hospitals + transit hubs + major hotels +
convention centers cluster. The frontend places one icon per cluster
centroid, with a tooltip listing the contributing landmarks.

Build once via:
  POST /admin/long_trip_hotspots/rebuild  (admin auth)

Read once via:
  GET /long_trip_hotspots  (user auth)

Refresh whenever you edit the POI list below.
"""

from __future__ import annotations

import json
import math
from typing import Any, Callable, Dict, List, Optional, Tuple

# (name, latitude, longitude, category, weight)
# Categories ranked by long-trip-generation intensity. The icon shown on
# the map will use the dominant category in each cluster.
NYC_LONG_TRIP_POIS: List[Tuple[str, float, float, str, float]] = [
    # Airports
    ("JFK Airport (central)",     40.6413, -73.7781, "airport", 5.0),
    ("LaGuardia Airport",         40.7769, -73.8740, "airport", 5.0),
    ("Newark Liberty Airport",    40.6895, -74.1745, "airport", 5.0),

    # Hospitals
    ("Mt Sinai Hospital (Main)",  40.7894, -73.9529, "hospital", 3.0),
    ("Mt Sinai West",             40.7706, -73.9876, "hospital", 3.0),
    ("Mt Sinai Morningside",      40.805859, -73.961664, "hospital", 3.0),
    ("Mt Sinai Beth Israel",      40.7325, -73.9824, "hospital", 3.0),
    ("NYU Langone Tisch",         40.7421, -73.9744, "hospital", 3.0),
    ("NYU Langone Brooklyn",      40.6453, -74.0193, "hospital", 3.0),
    ("Bellevue Hospital",         40.7392, -73.9759, "hospital", 3.0),
    ("Memorial Sloan Kettering",  40.7644, -73.9568, "hospital", 3.0),
    ("Hospital for Special Surgery", 40.765284, -73.953927, "hospital", 3.0),
    ("Lenox Hill Hospital",       40.7740, -73.9601, "hospital", 3.0),
    ("Columbia Presbyterian",     40.8418, -73.9419, "hospital", 3.0),
    ("NewYork-Presbyterian (LM)", 40.710404, -74.005493, "hospital", 3.0),
    ("NewYork-Presbyterian Queens", 40.747512, -73.826009, "hospital", 3.0),
    ("Brooklyn Hospital Center",  40.689646, -73.972209, "hospital", 3.0),
    ("Maimonides Medical Center", 40.63942, -73.998107, "hospital", 3.0),
    ("Coney Island Hospital",     40.586631, -73.96579, "hospital", 3.0),
    ("Elmhurst Hospital",         40.744775, -73.88565, "hospital", 3.0),
    ("Queens Hospital Center",    40.71749, -73.802239, "hospital", 3.0),
    ("Cohen Children's Medical",  40.752775, -73.707509, "hospital", 3.0),
    ("Montefiore Medical (Bronx)", 40.879983, -73.880739, "hospital", 3.0),
    ("Lincoln Hospital (Bronx)",  40.8175, -73.9251, "hospital", 3.0),
    ("Staten Island Univ Hospital", 40.583752, -74.086436, "hospital", 3.0),

    # Transit hubs
    ("Penn Station",              40.749103, -73.992005, "transit_hub", 2.5),
    ("Grand Central Terminal",    40.7527, -73.9772, "transit_hub", 2.5),
    ("Port Authority Bus Terminal", 40.7570, -73.9893, "transit_hub", 2.5),
    ("Atlantic Terminal (Brooklyn)", 40.6841, -73.9772, "transit_hub", 2.5),
    ("Jamaica Station (LIRR/AirTrain)", 40.700584, -73.807743, "transit_hub", 2.5),
    ("Hunters Point LIRR",        40.743804, -73.956153, "transit_hub", 2.0),
    ("Newark Penn Station",       40.7345, -74.1645, "transit_hub", 2.5),

    # Brooklyn — Sunset Park West waterfront (TLC zone 228). Quality
    # earnings anchors validated against live map data: ferry + work
    # campus that throw long, well-paid trips. (NYU Langone Brooklyn is
    # in the hospital block above.) Costco is deliberately NOT here — it
    # generates high pickup volume but short, cheap trips, so it reads as
    # a demand trap, not an earnings anchor.
    ("Brooklyn Army Terminal",    40.6447, -74.0238, "transit_hub", 2.0),
    ("Industry City",             40.6557, -74.0096, "corporate", 2.0),
    ("Industry City Food Hall",   40.656181, -74.00764, "luxury_shopping", 1.5),
    ("Japan Village",             40.6557, -74.006619, "luxury_shopping", 1.5),

    # Hotels
    ("Plaza Hotel",               40.7644, -73.9743, "hotel_luxury", 2.5),
    ("St Regis NY",               40.7615, -73.9742, "hotel_luxury", 2.5),
    ("Waldorf Astoria NY",        40.7560, -73.9744, "hotel_luxury", 2.5),
    ("The Pierre",                40.765441, -73.972072, "hotel_luxury", 2.3),
    ("Mandarin Oriental NY",      40.7686, -73.9819, "hotel_luxury", 2.5),
    ("Park Hyatt NY",             40.765005, -73.978661, "hotel_luxury", 2.3),
    ("Lotte NY Palace",           40.7585, -73.9742, "hotel_luxury", 2.3),
    ("Ritz-Carlton Central Park", 40.765073, -73.974971, "hotel_luxury", 2.5),
    ("Ritz-Carlton Battery Park", 40.7048, -74.0177, "hotel_luxury", 2.3),
    ("Four Seasons Tribeca",      40.712489, -74.009016, "hotel_luxury", 2.3),
    ("Marriott Marquis Times Sq", 40.7589, -73.9854, "hotel_luxury", 2.5),
    ("NY Hilton Midtown",         40.7621, -73.9789, "hotel_luxury", 2.5),
    ("Sheraton Times Square",     40.7625, -73.9826, "hotel_luxury", 2.3),
    ("Conrad NY Downtown",        40.7144, -74.0152, "hotel_luxury", 2.3),
    # Hotel densifiers (Midtown West, Midtown East, UES luxury, FiDi)
    ("W Times Square",            40.7601, -73.9849, "hotel_luxury", 2.2),
    ("The Knickerbocker",         40.7563, -73.9854, "hotel_luxury", 2.2),
    ("The Edition Times Square",  40.759232, -73.984535, "hotel_luxury", 2.3),
    ("Westin Times Square",       40.757501, -73.988493, "hotel_luxury", 2.2),
    ("Hilton Garden Inn Times Sq", 40.761059, -73.986911, "hotel_luxury", 2.0),
    ("Crowne Plaza Times Square", 40.760553, -73.984657, "hotel_luxury", 2.0),
    ("Westin NY Grand Central",   40.750581, -73.974162, "hotel_luxury", 2.2),
    ("The Peninsula NY",          40.7616, -73.9744, "hotel_luxury", 2.5),
    ("The Whitby Hotel",          40.7619, -73.9758, "hotel_luxury", 2.3),
    ("The Algonquin Hotel",       40.755631, -73.981799, "hotel_luxury", 2.0),
    ("Trump Tower",               40.7625, -73.9744, "hotel_luxury", 2.0),
    ("Trump International Hotel", 40.7685, -73.9819, "hotel_luxury", 2.3),
    ("Loews Regency NY",          40.764296, -73.968875, "hotel_luxury", 2.2),
    ("The Carlyle",               40.7747, -73.9633, "hotel_luxury", 2.5),
    ("The Mark Hotel",            40.775364, -73.963783, "hotel_luxury", 2.5),
    ("The Surrey",                40.7752, -73.9627, "hotel_luxury", 2.3),
    ("The Beekman Hotel",         40.7113, -74.0064, "hotel_luxury", 2.3),
    ("Four Seasons Downtown",     40.712489, -74.009016, "hotel_luxury", 2.3),
    ("11 Howard",                 40.7202, -73.9988, "hotel_luxury", 2.0),

    # Convention / performance / stadium
    ("Javits Center",             40.7577, -74.0024, "convention", 2.2),
    ("Madison Square Garden",     40.7505, -73.9934, "performance", 1.8),
    ("Lincoln Center",            40.77104, -73.981479, "performance", 1.5),
    ("Carnegie Hall",             40.7651, -73.9799, "performance", 1.5),
    ("Citi Field",                40.7571, -73.8458, "stadium", 1.8),
    ("USTA Billie Jean King",     40.7500, -73.8458, "stadium", 1.8),
    ("Barclays Center",           40.6826, -73.9754, "stadium", 1.8),

    # Corporate / financial
    ("NYSE / Wall St",            40.7069, -74.0113, "corporate", 1.8),
    ("Goldman Sachs HQ",          40.7150, -74.0144, "corporate", 1.8),
    ("Hudson Yards",              40.756108, -73.99972, "corporate", 1.8),
    ("Rockefeller Center",        40.7587, -73.9787, "corporate", 1.5),
    ("Bryant Park (corporate)",   40.7536, -73.9832, "corporate", 1.3),
    ("Bloomberg Tower",           40.762147, -73.968157, "corporate", 1.3),
    ("One Vanderbilt",            40.752974, -73.97854, "corporate", 1.5),
    ("Citigroup Center (601 Lex)", 40.7589, -73.9714, "corporate", 1.5),
    ("Time Warner Center",        40.7686, -73.9831, "corporate", 1.5),

    # Private clubs (wealthy / influential transit)
    ("Yale Club",                 40.7530, -73.9776, "private_club", 1.5),
    ("University Club",           40.7616, -73.9759, "private_club", 1.5),
    ("New York Athletic Club",    40.7659, -73.9772, "private_club", 1.5),

    # Luxury condos (high-end residents who take long trips out of town)
    ("432 Park Ave",              40.7616, -73.9716, "luxury_condo", 1.5),
    ("15 Central Park West",      40.769667, -73.980823, "luxury_condo", 1.5),
    ("220 Central Park South",    40.7662, -73.9803, "luxury_condo", 1.5),

    # Luxury shopping (high-net-worth shoppers heading home)
    ("Saks Fifth Ave",            40.7588, -73.9772, "luxury_shopping", 1.3),
    ("Bergdorf Goodman",          40.7635, -73.9742, "luxury_shopping", 1.3),
    ("Bloomingdale's flagship",   40.7625, -73.9684, "luxury_shopping", 1.3),
    ("Apple Fifth Ave",           40.7637, -73.9728, "luxury_shopping", 1.2),


    # ---------------- Park Ave / Midtown East corporate row
    # — institutional wealth, lots of car-service-out-of-town. ----------------
    ("JP Morgan HQ (270 Park)",   40.755849, -73.975037, "corporate", 1.8),
    ("MetLife Building (200 Park)", 40.7546, -73.9763, "corporate", 1.7),
    ("Seagram Building (375 Park)", 40.7585, -73.9722, "corporate", 1.6),
    ("Lever House (390 Park)",    40.7593, -73.9716, "corporate", 1.6),
    ("GM Building (767 5th)",     40.7635, -73.9729, "corporate", 1.6),
    ("General Electric Bldg (570 Lex)", 40.7572, -73.9728, "corporate", 1.6),
    ("Chrysler Building",         40.7516, -73.9755, "corporate", 1.6),
    ("Empire State Building",     40.7484, -73.9857, "corporate", 1.6),
    ("Park Ave Plaza (55 E 52)",  40.7587, -73.9737, "corporate", 1.5),
    ("245 Park Ave",              40.7556, -73.9749, "corporate", 1.5),
    ("280 Park Ave",              40.7563, -73.9743, "corporate", 1.5),

    # ---------------- Outer-borough additions ----------------
    # Brooklyn — Downtown Brooklyn (corporate, court system,
    # major transit). Atlantic Terminal + Barclays + Borough Hall +
    # 1 Hotel Brooklyn Bridge form the genuine downtown cluster.
    ("Brooklyn Borough Hall",     40.6925, -73.9899, "corporate", 1.5),
    ("1 Hotel Brooklyn Bridge",   40.70176, -73.995504, "hotel_luxury", 2.2),
    ("MetroTech Center",          40.6943, -73.9870, "corporate", 1.6),
    ("Brooklyn Marriott Bridge",  40.692366, -73.988815, "hotel_luxury", 2.0),
    ("Aloft Brooklyn",            40.6928, -73.9854, "hotel_luxury", 1.8),
    ("Sheraton Brooklyn",         40.691577, -73.984443, "hotel_luxury", 1.9),

    # DUMBO — Etsy HQ + Dock 72 corporate (they join the 1 Hotel Brooklyn
    # Bridge waterfront cluster; the duplicate "1 Hotel DUMBO" entry was removed).
    # Dropped Empire Stores / Time Out Market / St Ann's — they're tourist
    # foot-traffic or niche performance, not wealth or car-service volume.
    ("Etsy HQ (DUMBO)",           40.700415, -73.988628, "corporate", 1.6),
    ("Dock 72 (Brooklyn Navy Yard)", 40.698067, -73.974935, "corporate", 1.5),

    # Williamsburg — boutique hotels + nightlife wealth
    ("William Vale Hotel",        40.7218, -73.9568, "hotel_luxury", 2.0),
    ("Wythe Hotel",               40.7223, -73.9577, "hotel_luxury", 2.0),
    ("Hoxton Williamsburg",       40.7210, -73.9573, "hotel_luxury", 1.9),
    ("McCarren Hotel & Pool",     40.720874, -73.955312, "hotel_luxury", 1.8),

    # LIC corporate (Citi tower, JetBlue HQ, JACX) — actual finance + tech wealth
    ("Citigroup Tower (LIC)",     40.7475, -73.9420, "corporate", 1.8),
    ("JetBlue HQ (Brewster Bldg)", 40.750627, -73.939341, "corporate", 1.7),
    ("JACX Queens Plaza",         40.748355, -73.939134, "corporate", 1.6),
    ("Court Square (LIC)",        40.745818, -73.94565, "transit_hub", 1.8),
    ("Boro Hotel LIC",            40.755106, -73.935397, "hotel_luxury", 1.7),
    ("Z NYC Hotel",               40.751855, -73.947948, "hotel_luxury", 1.6),
    ("Ravel Hotel LIC",           40.75411, -73.949273, "hotel_luxury", 1.5),
    # LIC / Dutch Kills hotel district near Queens Plaza — a dense cluster
    # of airport hotels (LaGuardia proximity), heavy on early-AM airport
    # runs. Coordinates from OpenStreetMap.
    ("Holiday Inn LIC",           40.753305, -73.934409, "hotel_luxury", 1.6),
    ("Ramada LIC",                40.755235, -73.936721, "hotel_luxury", 1.5),
    ("Home2 Suites LIC",          40.753022, -73.934104, "hotel_luxury", 1.5),
    ("Fairfield Inn LIC",         40.751451, -73.935117, "hotel_luxury", 1.5),
    ("Country Inn & Suites LIC",  40.753067, -73.938488, "hotel_luxury", 1.5),
    ("Comfort Inn LIC",           40.757614, -73.938415, "hotel_luxury", 1.5),
    ("Quality Inn LIC",           40.752017, -73.934476, "hotel_luxury", 1.5),
    ("Best Western LIC",          40.755938, -73.941157, "hotel_luxury", 1.5),

    # Astoria — Mt Sinai Astoria (hospital, high-traffic). Dropped
    # Kaufman Studios + Museum of Moving Image — working soundstage and
    # niche museum, neither produces wealthy long-trip riders.
    ("Mt Sinai Astoria",          40.768407, -73.924769, "hospital", 2.0),

    # Brooklyn — Crown Heights / Bed-Stuy hospitals (real medical volume).
    ("Kings County Hospital",     40.655772, -73.945226, "hospital", 2.5),
    ("SUNY Downstate Medical",    40.655674, -73.944953, "hospital", 2.5),


    # Nassau/LI — Long Island Jewish + Northwell hospitals (huge medical
    # campuses, but spread too far apart to cluster — stay filtered POIs).
    ("Long Island Jewish Hospital", 40.753334, -73.706883, "hospital", 2.8),
    ("Northwell North Shore Univ", 40.7766, -73.7045, "hospital", 2.5),
    ("Northwell Imaging Manhasset", 40.793944, -73.688914, "hospital", 1.8),

    # Hoboken — Terminal (PATH+NJT, very high traffic) + W Hotel.
    # Dropped Pier 13 — outdoor open-air beer garden, summer-only,
    # not a building.
    ("Hoboken Terminal",          40.7359, -74.0291, "transit_hub", 2.3),
    ("W Hoboken",                 40.738688, -74.028269, "hotel_luxury", 2.0),

    # Jersey City — Exchange Place financial cluster
    ("Goldman Sachs Tower (JC)",  40.712772, -74.034695, "corporate", 1.8),
    ("Hyatt Regency Jersey City", 40.7166, -74.0339, "hotel_luxury", 2.0),
    ("Exchange Place PATH",       40.7162, -74.0335, "transit_hub", 2.2),
    ("Harborside Plaza",          40.718824, -74.033906, "corporate", 1.6),
    ("Newport Centre / PATH",     40.726798, -74.0387, "transit_hub", 2.0),
    ("W Hotel JC (Newport)",      40.7271, -74.0354, "hotel_luxury", 2.0),

    # Manhattan — Tribeca / Soho boutique hotels (often missed)
    ("Greenwich Hotel",           40.7195, -74.0107, "hotel_luxury", 2.2),
    ("Roxy Hotel",                40.7203, -74.0046, "hotel_luxury", 1.9),
    ("Mr C Seaport",              40.708098, -74.001631, "hotel_luxury", 1.8),
    ("Crosby Street Hotel",       40.7233, -73.9989, "hotel_luxury", 2.1),
    ("Mercer Hotel",              40.7242, -73.9989, "hotel_luxury", 2.0),
    ("Soho Grand Hotel",          40.721693, -74.00444, "hotel_luxury", 1.9),

    # Manhattan — Chelsea / Meatpacking luxury hotels. Dropped The High
    # Line (public elevated park, not a building). Hotel Chelsea kept —
    # post-renovation it's a true luxury hotel again.
    ("Standard High Line",        40.7409, -74.0080, "hotel_luxury", 2.1),
    ("Gansevoort Meatpacking",    40.740128, -74.005879, "hotel_luxury", 1.9),
    ("Hotel Chelsea",             40.7440, -73.9968, "hotel_luxury", 1.8),

    # Manhattan — Flatiron / NoMad luxury hotels. Dropped Madison Square
    # Park (public park) and Eataly (tourist food hall). Flatiron Bldg
    # kept — it's a real high-traffic office + ground-floor retail tower.
    ("Flatiron Building",         40.7411, -73.9897, "corporate", 1.4),
    ("Ace Hotel NY",              40.7464, -73.9883, "hotel_luxury", 1.9),
    ("NoMad Hotel",               40.7444, -73.9879, "hotel_luxury", 2.0),
    ("The James NoMad",           40.744664, -73.985478, "hotel_luxury", 1.8),
    ("Marriott Edition Madison",  40.741174, -73.987688, "hotel_luxury", 1.9),

    # Manhattan — UWS hotels + Lincoln Center cluster densifier
    ("Empire Hotel",              40.7720, -73.9826, "hotel_luxury", 1.9),
    ("Hotel Beacon",              40.7807, -73.9803, "hotel_luxury", 1.7),
    ("NYU Langone Hospital UWS",  40.772116, -73.987611, "hospital", 2.5),

    # Manhattan — Financial District densifier (existing hotels + offices)
    ("Hotel AKA Wall St",         40.708081, -74.007808, "hotel_luxury", 1.9),
    ("Wall Street Inn",           40.7041, -74.0112, "hotel_luxury", 1.7),
    ("Cipriani Wall Street",      40.7059, -74.0099, "performance", 1.4),
    ("70 Pine Street",            40.7062, -74.0079, "corporate", 1.5),

    # ---------------- Outer-borough cluster expansion ----------------
    # The earlier outer-borough POIs left the Bronx and Staten Island
    # with zero flags and several lone landmarks (Yankee Stadium, Lincoln/
    # Kings County hospitals, Atlantic Terminal/Barclays, 1 Hotel Brooklyn Bridge)
    # stranded as singletons/pairs. These additions complete those into
    # genuine 3+ clusters using the "busy buildings" bar where outright
    # wealth is sparse — major transit, medical complexes, civic/retail.

    # Queens — Downtown Flushing (transit hub + malls + hotel + office)
    ("Flushing-Main St (7 train)", 40.7596, -73.8302, "transit_hub", 2.3),
    ("Sheraton LaGuardia East",   40.759748, -73.831895, "hotel_luxury", 1.8),
    ("New World Mall (Flushing)", 40.7597, -73.829377, "luxury_shopping", 1.5),
    ("Tangram (Flushing)",        40.759238, -73.833945, "luxury_shopping", 1.5),
    ("One Fulton Square (Flushing)", 40.7595, -73.832325, "corporate", 1.4),


    # Brooklyn — Atlantic Yards (completes Atlantic Terminal + Barclays)
    ("1 Hanson Place (Brooklyn)", 40.6846, -73.9776, "luxury_condo", 1.5),
    ("Atlantic Center (mall)",    40.6839, -73.9766, "luxury_shopping", 1.4),

    # Brooklyn — DUMBO (completes the two 1 Hotel Brooklyn Bridge entries)
    ("Olympia Dumbo (condos)",    40.7028, -73.9899, "luxury_condo", 1.5),
    ("10 Jay St (DUMBO offices)", 40.704506, -73.986635, "corporate", 1.4),

    # Brooklyn — Crown Heights medical (completes Kings County + Downstate)
    ("University Hospital of Brooklyn", 40.654458, -73.946245, "hospital", 2.5),

    # Bronx — The Hub / South Bronx (completes Lincoln Hospital)
    ("149 St-Grand Concourse (subway)", 40.8183, -73.9268, "transit_hub", 2.0),
    ("Bronx General Post Office", 40.819013, -73.927017, "corporate", 1.3),



    # ---------------- Hudson Yards / Manhattan West (gives the existing
    # "Hudson Yards" corporate POI its district: HQ towers, Equinox hotel,
    # 7-train terminus — heavy business-traveler + airport volume) -----------
    ("Equinox Hotel Hudson Yards", 40.7540, -74.0014, "hotel_luxury", 2.3),
    ("30 Hudson Yards",           40.7538, -74.0008, "corporate", 1.7),
    ("55 Hudson Yards",           40.755144, -74.000628, "corporate", 1.6),

    # ---------------- Battery Park City / Brookfield Place (anchors the
    # existing Conrad + Goldman Sachs HQ with the Brookfield luxury mall) -----
    ("Brookfield Place",          40.7128, -74.0155, "luxury_shopping", 1.6),

    # ---------------- Meatpacking / High Line (completes the existing Standard
    # High Line + Gansevoort with the Whitney + luxury retail) ---------------
    ("Whitney Museum",            40.7395, -74.0090, "tourist", 1.3),
    ("RH Meatpacking",            40.7404, -74.0072, "luxury_shopping", 1.3),

    # ---------------- "Near-miss" completions: a single addition that turns an
    # existing 2-building pair into a genuine 3+ Strategic Point ----------------
    # UES elite medical: completes Memorial Sloan Kettering + Hospital for Special Surgery
    ("NewYork-Presbyterian/Weill Cornell", 40.7648, -73.9543, "hospital", 3.0),
    # UES elite hotels: completes The Pierre + Loews Regency
    ("The Lowell Hotel",          40.7656, -73.9690, "hotel_luxury", 2.3),
    # Penn district transit: completes Penn Station + Madison Square Garden
    ("Moynihan Train Hall",       40.7506, -73.9968, "transit_hub", 2.5),
    # UWS: completes Hotel Beacon + NYU Langone UWS
    ("Beacon Theatre",            40.7800, -73.9817, "performance", 1.6),
    # Rockefeller Center: completes Rockefeller Center + Saks Fifth Ave
    ("Radio City Music Hall",     40.7600, -73.9800, "performance", 1.6),
    # Bryant Park: NYPL + 2 hotels on the 40th-St side (the existing Bryant
    # Park corporate POI is greedily taken by the Algonquin, so this trio
    # stands on its own)
    ("New York Public Library",   40.7532, -73.9822, "tourist", 1.4),
    ("Bryant Park Hotel",         40.7521, -73.9836, "hotel_luxury", 1.9),
    ("Refinery Hotel",            40.7516, -73.9844, "hotel_luxury", 1.8),

    # ================= BROOKLYN & QUEENS expansion =================
    # Brooklyn — Downtown Brooklyn East (City Point / DeKalb): dense retail +
    # supertall luxury condo + subway hub + hotel, distinct from the Borough
    # Hall / MetroTech cluster ~0.4 mi west.
    ("City Point Brooklyn",       40.6904, -73.9826, "luxury_shopping", 1.5),
    ("The Brooklyn Tower",        40.6900, -73.9831, "luxury_condo", 1.7),
    ("DeKalb Av (B/Q/R)",         40.6905, -73.9819, "transit_hub", 1.8),
    ("Hotel Indigo Downtown Bklyn", 40.691573, -73.984292, "hotel_luxury", 1.7),
    ("Ava DoBro",                 40.6917, -73.9836, "luxury_condo", 1.5),

    # condo towers + East River state park + the Hunters Point library.
]


# Clustering radius: complete-link cap on the max pair-distance
# between any two members of a cluster. 0.25 mi ≈ 5 min walk at a
# normal 3 mph pace — the driver-stated bar for "close enough to
# qualify as one hotspot". Triangle inequality means every member is
# within 5 min of the centroid (and the snapped flag) too, so the
# popup's "buildings represented" list is also the buildings any
# passenger could walk to from the flag in 5 min.
CLUSTER_RADIUS_MI = 0.25

# Minimum POIs in a cluster for it to count as a hotspot. The whole
# point is "a SPOT where 3+ important buildings are nearby" — single
# isolated landmarks and lone pairs aren't significant enough to mark
# on the map.
MIN_MEMBERS_PER_HOTSPOT = 3

# Category priority for the cluster-icon label. Higher index in this
# list wins when a cluster has multiple categories. Drivers care most
# about airports → hospitals → transit → hotels, so airports win the
# label if any airport POI is in the cluster.
_CATEGORY_PRIORITY: List[str] = [
    # Event-dependent venues are kept LOWEST. A theater/arena is dark most
    # nights, so its "post-show" window must never drive a nightly pulse when a
    # reliable daily category is present — otherwise a cluster like Rockefeller
    # Center would falsely pulse at show-let-out time on nights with no show.
    # A reliable daily category (corporate / hotel / transit / hospital /
    # shopping) wins instead; an event category only dominates a cluster that
    # has nothing else (and such pure-event clusters are intentionally dropped).
    "performance", "stadium",
    "tourist", "luxury_shopping", "luxury_condo", "private_club",
    "private_school", "corporate", "convention",
    "hotel_luxury", "transit_hub", "hospital", "airport",
]


# Best hours by category — when a driver standing near this kind of
# building is most likely to get long-trip pickups. These are heuristics
# based on building-type patterns, not measured trip data; they're a
# hint for the driver, not a guarantee.
BEST_HOURS_BY_CATEGORY: Dict[str, str] = {
    "airport":         "24/7 — peaks 5–8am, 4–9pm",
    "hospital":        "24/7 — discharges peak 10am–4pm",
    "hotel_luxury":    "Checkout 7am–noon — airport runs (check-in isn't a pickup)",
    "transit_hub":     "Weekday rush 7–9am, 5–7pm",
    "corporate":       "Weekday end-of-day 4–8pm (esp. Thu/Fri); closed holidays",
    "private_school":  "Weekday pickup 2:30–4pm; closed weekends, holidays & summer",
    "private_club":    "Lunch 12–2pm, dinner 6–9pm",
    "luxury_condo":    "Weekday morning 7–9am",
    "luxury_shopping": "11am–7pm, peak Sat 1–6pm",
    "performance":     "1–3 hours after curtain",
    "stadium":         "1–2 hours after game ends",
    "convention":      "9am–6pm during events",
    "tourist":         "10am–6pm",
}


# Street addresses for the POIs. Hand-curated from common knowledge of
# the most-visited NYC buildings. Where I'm not confident in a precise
# street number, the value is a cross-street / neighborhood description
# instead of a guess — better vague-but-correct than precise-but-wrong.
# Drivers using the popup should treat the address as a navigation hint,
# not gospel.
POI_ADDRESSES: Dict[str, str] = {
    # Hudson Yards / Manhattan West
    "Equinox Hotel Hudson Yards": "33 Hudson Yards, NY 10001",
    "30 Hudson Yards": "30 Hudson Yards, NY 10001",
    "55 Hudson Yards": "550 W 34th St, NY 10001",
    # Battery Park City / Brookfield Place
    "Brookfield Place": "230 Vesey St, NY 10281",
    # Meatpacking / High Line
    "Whitney Museum": "99 Gansevoort St, NY 10014",
    "RH Meatpacking": "9-19 9th Ave, NY 10014",
    # Near-miss completions
    "NewYork-Presbyterian/Weill Cornell": "525 E 68th St, NY 10065",
    "The Lowell Hotel": "28 E 63rd St, NY 10065",
    "Moynihan Train Hall": "351 W 31st St, NY 10001",
    "Beacon Theatre": "2124 Broadway, NY 10023",
    "Radio City Music Hall": "1260 6th Ave, NY 10020",
    "New York Public Library": "476 5th Ave, NY 10018",
    "Bryant Park Hotel": "40 W 40th St, NY 10018",
    "Refinery Hotel": "63 W 38th St, NY 10018",
    # Brooklyn — Downtown Brooklyn East
    "City Point Brooklyn": "445 Albee Square W, Brooklyn 11201",
    "The Brooklyn Tower": "9 DeKalb Ave, Brooklyn 11201",
    "DeKalb Av (B/Q/R)": "DeKalb Ave & Flatbush Ave Ext, Brooklyn 11201",
    "Hotel Indigo Downtown Bklyn": "229 Duffield St, Brooklyn 11201",
    "Ava DoBro": "100 Willoughby St, Brooklyn 11201",
    # Queens — LIC waterfront
    # Airports
    "JFK Airport (central)": "JFK Airport, Jamaica, NY 11430",
    "LaGuardia Airport": "LaGuardia Airport, Flushing, NY 11371",
    "Newark Liberty Airport": "Newark Liberty Intl Airport, Newark, NJ 07114",
    # Hospitals
    "Mt Sinai Hospital (Main)": "1 Gustave L. Levy Place, NY 10029",
    "Mt Sinai West": "1000 10th Ave, NY 10019",
    "Mt Sinai Morningside": "1111 Amsterdam Ave, NY 10025",
    "Mt Sinai Beth Israel": "281 1st Ave, NY 10003",
    "NYU Langone Tisch": "550 1st Ave, NY 10016",
    "NYU Langone Brooklyn": "150 55th St, Brooklyn 11220",
    "Brooklyn Army Terminal": "140 58th St, Brooklyn 11220",
    "Industry City": "220 36th St, Brooklyn 11232",
    "Industry City Food Hall": "254 36th St, Brooklyn 11232",
    "Japan Village": "934 3rd Ave, Brooklyn 11232",
    "Bellevue Hospital": "462 1st Ave, NY 10016",
    "Memorial Sloan Kettering": "1275 York Ave, NY 10065",
    "Hospital for Special Surgery": "535 E 70th St, NY 10021",
    "Lenox Hill Hospital": "100 E 77th St, NY 10075",
    "Columbia Presbyterian": "622 W 168th St, NY 10032",
    "NewYork-Presbyterian (LM)": "170 William St, NY 10038",
    "NewYork-Presbyterian Queens": "56-45 Main St, Flushing 11355",
    "Brooklyn Hospital Center": "121 DeKalb Ave, Brooklyn 11201",
    "Maimonides Medical Center": "4802 10th Ave, Brooklyn 11219",
    "Coney Island Hospital": "2601 Ocean Pkwy, Brooklyn 11235",
    "Elmhurst Hospital": "79-01 Broadway, Elmhurst 11373",
    "Queens Hospital Center": "82-68 164th St, Jamaica 11432",
    "Cohen Children's Medical": "269-01 76th Ave, New Hyde Park 11040",
    "Montefiore Medical (Bronx)": "111 E 210th St, Bronx 10467",
    "Lincoln Hospital (Bronx)": "234 E 149th St, Bronx 10451",
    "Staten Island Univ Hospital": "475 Seaview Ave, Staten Island 10305",
    "Mt Sinai Astoria": "25-10 30th Ave, Astoria 11102",
    "Kings County Hospital": "451 Clarkson Ave, Brooklyn 11203",
    "SUNY Downstate Medical": "450 Clarkson Ave, Brooklyn 11203",
    "Long Island Jewish Hospital": "270-05 76th Ave, New Hyde Park 11040",
    "Northwell North Shore Univ": "300 Community Dr, Manhasset 11030",
    "Northwell Imaging Manhasset": "1554 Northern Blvd, Manhasset 11030",
    "NYU Langone Hospital UWS": "211 W 61st St, NY 10023",
    # Transit hubs
    "Penn Station": "31st St & 7th Ave, NY 10001",
    "Grand Central Terminal": "89 E 42nd St, NY 10017",
    "Port Authority Bus Terminal": "625 8th Ave, NY 10018",
    "Atlantic Terminal (Brooklyn)": "139 Flatbush Ave, Brooklyn 11217",
    "Jamaica Station (LIRR/AirTrain)": "Sutphin Blvd & Archer Ave, Jamaica 11435",
    "Hunters Point LIRR": "49-01 5th St, LIC 11101",
    "Newark Penn Station": "1 Raymond Plaza W, Newark, NJ 07102",
    "Court Square (LIC)": "Jackson Ave & 23rd St, LIC 11101",
    "Hoboken Terminal": "1 Hudson Pl, Hoboken, NJ 07030",
    "Exchange Place PATH": "10 Exchange Pl, Jersey City, NJ 07302",
    "Newport Centre / PATH": "30 Mall Dr W, Jersey City, NJ 07310",
    # Hotels — luxury
    "Plaza Hotel": "768 5th Ave, NY 10019",
    "St Regis NY": "2 E 55th St, NY 10022",
    "Waldorf Astoria NY": "301 Park Ave, NY 10022",
    "The Pierre": "2 E 61st St, NY 10065",
    "Mandarin Oriental NY": "80 Columbus Cir, NY 10023",
    "Park Hyatt NY": "153 W 57th St, NY 10019",
    "Lotte NY Palace": "455 Madison Ave, NY 10022",
    "Ritz-Carlton Central Park": "50 Central Park S, NY 10019",
    "Ritz-Carlton Battery Park": "2 West St, NY 10004",
    "Four Seasons Tribeca": "27 Barclay St, NY 10007",
    "Marriott Marquis Times Sq": "1535 Broadway, NY 10036",
    "NY Hilton Midtown": "1335 6th Ave, NY 10019",
    "Sheraton Times Square": "811 7th Ave, NY 10019",
    "Conrad NY Downtown": "102 N End Ave, NY 10282",
    "W Times Square": "1567 Broadway, NY 10036",
    "The Knickerbocker": "6 Times Sq, NY 10036",
    "The Edition Times Square": "701 7th Ave, NY 10036",
    "Westin Times Square": "270 W 43rd St, NY 10036",
    "Hilton Garden Inn Times Sq": "790 8th Ave, NY 10019",
    "Crowne Plaza Times Square": "1605 Broadway, NY 10019",
    "Westin NY Grand Central": "212 E 42nd St, NY 10017",
    "The Peninsula NY": "700 5th Ave, NY 10019",
    "The Whitby Hotel": "18 W 56th St, NY 10019",
    "The Algonquin Hotel": "59 W 44th St, NY 10036",
    "Trump Tower": "725 5th Ave, NY 10022",
    "Trump International Hotel": "1 Central Park W, NY 10023",
    "Loews Regency NY": "540 Park Ave, NY 10065",
    "The Carlyle": "35 E 76th St, NY 10021",
    "The Mark Hotel": "25 E 77th St, NY 10075",
    "The Surrey": "20 E 76th St, NY 10021",
    "The Beekman Hotel": "123 Nassau St, NY 10038",
    "Four Seasons Downtown": "27 Barclay St, NY 10007",
    "11 Howard": "11 Howard St, NY 10013",
    "1 Hotel Brooklyn Bridge": "60 Furman St, Brooklyn 11201",
    "Brooklyn Marriott Bridge": "333 Adams St, Brooklyn 11201",
    "Aloft Brooklyn": "216 Duffield St, Brooklyn 11201",
    "Sheraton Brooklyn": "228 Duffield St, Brooklyn 11201",
    "Brooklyn Bridge Marriott": "333 Adams St, Brooklyn 11201",
    "1 Hotel Brooklyn (DUMBO)": "60 Furman St, Brooklyn 11201",
    "William Vale Hotel": "111 N 12th St, Brooklyn 11249",
    "Wythe Hotel": "80 Wythe Ave, Brooklyn 11249",
    "Hoxton Williamsburg": "97 Wythe Ave, Brooklyn 11249",
    "McCarren Hotel & Pool": "160 N 12th St, Brooklyn 11249",
    "Boro Hotel LIC": "38-28 27th St, LIC 11101",
    "Z NYC Hotel": "11-01 43rd Ave, LIC 11101",
    "Ravel Hotel LIC": "8-08 Queens Plaza S, LIC 11101",
    "Holiday Inn LIC": "Dutch Kills, Long Island City 11101",
    "Ramada LIC": "Dutch Kills, Long Island City 11101",
    "Home2 Suites LIC": "Dutch Kills, Long Island City 11101",
    "Fairfield Inn LIC": "Dutch Kills, Long Island City 11101",
    "Country Inn & Suites LIC": "Dutch Kills, Long Island City 11101",
    "Comfort Inn LIC": "Dutch Kills, Long Island City 11101",
    "Quality Inn LIC": "Dutch Kills, Long Island City 11101",
    "Best Western LIC": "Dutch Kills, Long Island City 11101",
    "W Hoboken": "225 River St, Hoboken, NJ 07030",
    "Hyatt Regency Jersey City": "2 Exchange Pl, Jersey City, NJ 07302",
    "W Hotel JC (Newport)": "541 Washington Blvd, Jersey City, NJ 07310",
    "Greenwich Hotel": "377 Greenwich St, NY 10013",
    "Roxy Hotel": "2 6th Ave, NY 10013",
    "Mr C Seaport": "33 Peck Slip, NY 10038",
    "Crosby Street Hotel": "79 Crosby St, NY 10012",
    "Mercer Hotel": "147 Mercer St, NY 10012",
    "Soho Grand Hotel": "310 W Broadway, NY 10013",
    "Standard High Line": "848 Washington St, NY 10014",
    "Gansevoort Meatpacking": "18 9th Ave, NY 10014",
    "Hotel Chelsea": "222 W 23rd St, NY 10011",
    "Ace Hotel NY": "20 W 29th St, NY 10001",
    "NoMad Hotel": "1170 Broadway, NY 10001",
    "The James NoMad": "22 E 29th St, NY 10016",
    "Marriott Edition Madison": "5 Madison Ave, NY 10010",
    "Empire Hotel": "44 W 63rd St, NY 10023",
    "Hotel Beacon": "2130 Broadway, NY 10023",
    "Hotel AKA Wall St": "84 William St, NY 10038",
    "Wall Street Inn": "9 S William St, NY 10004",
    # Convention / performance / stadium
    "Javits Center": "429 11th Ave, NY 10001",
    "Madison Square Garden": "4 Pennsylvania Plaza, NY 10001",
    "Lincoln Center": "10 Lincoln Center Plaza, NY 10023",
    "Carnegie Hall": "881 7th Ave, NY 10019",
    "Citi Field": "41 Seaver Way, Flushing 11368",
    "USTA Billie Jean King": "Flushing Meadows-Corona Park, Flushing 11368",
    "Barclays Center": "620 Atlantic Ave, Brooklyn 11217",
    "Cipriani Wall Street": "55 Wall St, NY 10005",
    # Corporate / financial
    "NYSE / Wall St": "11 Wall St, NY 10005",
    "Goldman Sachs HQ": "200 West St, NY 10282",
    "Hudson Yards": "20 Hudson Yards, NY 10001",
    "Rockefeller Center": "45 Rockefeller Plaza, NY 10111",
    "Bryant Park (corporate)": "1095 6th Ave (Bank of America Tower), NY 10036",
    "Bloomberg Tower": "731 Lexington Ave, NY 10022",
    "One Vanderbilt": "1 Vanderbilt Ave, NY 10017",
    "Citigroup Center (601 Lex)": "601 Lexington Ave, NY 10022",
    "Time Warner Center": "10 Columbus Cir, NY 10019",
    "JP Morgan HQ (270 Park)": "270 Park Ave, NY 10017",
    "MetLife Building (200 Park)": "200 Park Ave, NY 10166",
    "Seagram Building (375 Park)": "375 Park Ave, NY 10152",
    "Lever House (390 Park)": "390 Park Ave, NY 10022",
    "GM Building (767 5th)": "767 5th Ave, NY 10153",
    "General Electric Bldg (570 Lex)": "570 Lexington Ave, NY 10022",
    "Chrysler Building": "405 Lexington Ave, NY 10174",
    "Empire State Building": "20 W 34th St, NY 10001",
    "Park Ave Plaza (55 E 52)": "55 E 52nd St, NY 10055",
    "245 Park Ave": "245 Park Ave, NY 10167",
    "280 Park Ave": "280 Park Ave, NY 10017",
    "Brooklyn Borough Hall": "209 Joralemon St, Brooklyn 11201",
    "MetroTech Center": "2 MetroTech Center, Brooklyn 11201",
    "Etsy HQ (DUMBO)": "117 Adams St, Brooklyn 11201",
    "Dock 72 (Brooklyn Navy Yard)": "63 Flushing Ave, Brooklyn 11205",
    "Citigroup Tower (LIC)": "1 Court Square, LIC 11101",
    "JetBlue HQ (Brewster Bldg)": "27-01 Queens Plaza N, LIC 11101",
    "JACX Queens Plaza": "28-07 Jackson Ave, LIC 11101",
    "Flatiron Building": "175 5th Ave, NY 10010",
    "Goldman Sachs Tower (JC)": "30 Hudson St, Jersey City, NJ 07302",
    "Harborside Plaza": "210 Hudson St, Jersey City, NJ 07311",
    "70 Pine Street": "70 Pine St, NY 10005",
    # Private clubs
    "Yale Club": "50 Vanderbilt Ave, NY 10017",
    "University Club": "1 W 54th St, NY 10019",
    "New York Athletic Club": "180 Central Park S, NY 10019",
    # Luxury condos
    "432 Park Ave": "432 Park Ave, NY 10022",
    "15 Central Park West": "15 Central Park W, NY 10023",
    "220 Central Park South": "220 Central Park S, NY 10019",
    # Luxury shopping
    "Saks Fifth Ave": "611 5th Ave, NY 10022",
    "Bergdorf Goodman": "754 5th Ave, NY 10019",
    "Bloomingdale's flagship": "1000 3rd Ave, NY 10022",
    "Apple Fifth Ave": "767 5th Ave, NY 10153",
    # Private schools (UES)
    "Dalton School": "108 E 89th St, NY 10128",
    "Spence School": "22 E 91st St, NY 10128",
    "Brearley School": "610 E 83rd St, NY 10028",
    "Chapin School": "100 East End Ave, NY 10028",
    "Buckley School": "113 E 73rd St, NY 10021",
    "Trinity School (UWS)": "139 W 91st St, NY 10024",
    "Collegiate School": "301 Freedom Pl S, NY 10069",
    "Nightingale-Bamford": "20 E 92nd St, NY 10128",
    "Marymount School NY": "1026 5th Ave, NY 10028",
    # Bronx private schools
    "Riverdale Country School": "5250 Fieldston Rd, Bronx 10471",
    "Horace Mann School (Bronx)": "231 W 246th St, Bronx 10471",
    # Outer-borough cluster expansion
    "Flushing-Main St (7 train)": "Main St & Roosevelt Ave, Flushing 11354",
    "Sheraton LaGuardia East": "135-20 39th Ave, Flushing 11354",
    "New World Mall (Flushing)": "136-20 Roosevelt Ave, Flushing 11354",
    "Tangram (Flushing)": "133-27 39th Ave, Flushing 11354",
    "One Fulton Square (Flushing)": "39-16 Prince St, Flushing 11354",
    "1 Hanson Place (Brooklyn)": "1 Hanson Pl, Brooklyn 11217",
    "Atlantic Center (mall)": "625 Atlantic Ave, Brooklyn 11217",
    "Olympia Dumbo (condos)": "30 Front St, Brooklyn 11201",
    "10 Jay St (DUMBO offices)": "10 Jay St, Brooklyn 11201",
    "University Hospital of Brooklyn": "445 Lenox Rd, Brooklyn 11203",
    "149 St-Grand Concourse (subway)": "E 149th St & Grand Concourse, Bronx 10451",
    "Bronx General Post Office": "558 Grand Concourse, Bronx 10451",
}


def summarize_categories(categories: List[str]) -> Tuple[Dict[str, int], str]:
    """
    Returns (counts_by_category, human_rationale_string) from a flat list
    of category strings.

    The rationale string is what the popup shows the driver — a plain
    English summary of why this cluster qualifies as a hotspot. Shared by
    the build path (cluster member indices) and the read path (members
    loaded back from members_json) so both produce identical wording.
    """
    counts: Dict[str, int] = {}
    for cat in categories:
        if not cat:
            continue
        counts[cat] = counts.get(cat, 0) + 1

    pretty = {
        "airport":         ("airport", "airports"),
        "hospital":        ("major hospital", "major hospitals"),
        "hotel_luxury":    ("luxury hotel", "luxury hotels"),
        "transit_hub":     ("transit hub", "transit hubs"),
        "corporate":       ("corporate tower", "corporate towers"),
        "private_school":  ("elite private school", "elite private schools"),
        "private_club":    ("private club", "private clubs"),
        "luxury_condo":    ("luxury condo", "luxury condos"),
        "luxury_shopping": ("luxury retail flagship", "luxury retail flagships"),
        "performance":     ("performance venue", "performance venues"),
        "stadium":         ("stadium", "stadiums"),
        "convention":      ("convention venue", "convention venues"),
        "tourist":         ("tourist landmark", "tourist landmarks"),
    }
    # Order parts by category priority so the most-important type is
    # named first ("1 major hospital + 4 luxury hotels"), not last.
    order = list(reversed(_CATEGORY_PRIORITY))
    parts: List[str] = []
    for cat in order:
        n = counts.get(cat, 0)
        if not n:
            continue
        singular, plural = pretty.get(cat, (cat, cat))
        parts.append(f"{n} {singular if n == 1 else plural}")
    rationale = " + ".join(parts) if parts else "high-traffic cluster"
    return counts, rationale


def _category_summary(member_indices: List[int]) -> Tuple[Dict[str, int], str]:
    """Counts + rationale for a cluster given its POI member indices."""
    return summarize_categories([NYC_LONG_TRIP_POIS[i][3] for i in member_indices])


def _nearest_member_to_centroid(
    indices: List[int], lat_c: float, lng_c: float,
) -> Tuple[float, float]:
    """
    Pick the member POI closest to the weighted centroid and return its
    (lat, lng). The raw centroid is mathematically the "middle" but it
    can land in the middle of an intersection or on a building face the
    driver can't reach. Snapping to the nearest real building gives a
    physically-meaningful stand spot.
    """
    best_idx = indices[0]
    best_d = float("inf")
    for i in indices:
        lat_i = NYC_LONG_TRIP_POIS[i][1]
        lng_i = NYC_LONG_TRIP_POIS[i][2]
        d = haversine_miles(lat_c, lng_c, lat_i, lng_i)
        if d < best_d:
            best_d = d
            best_idx = i
    return NYC_LONG_TRIP_POIS[best_idx][1], NYC_LONG_TRIP_POIS[best_idx][2]


def _poi_address(name: str) -> str:
    """Return the street address for a POI, or a neutral fallback."""
    return POI_ADDRESSES.get(name, "Address not listed")


def _cluster_has_hospital(member_indices: List[int]) -> bool:
    """True if any POI in the cluster is a hospital — a 24/7 trip generator
    that is a valid strategic point even when it stands nearly alone."""
    return any(NYC_LONG_TRIP_POIS[i][3] == "hospital" for i in member_indices)


def _best_hours_for(category: str) -> str:
    return BEST_HOURS_BY_CATEGORY.get(category, "Varies")


# Per-category dim schedule for the dollar-flag time-of-day signal.
#
# - `peak`:  hour ranges [start_h, end_h_exclusive] in NYC local time
#   when the flag should be at full brightness.
# - `off`:   hour ranges when the flag should be dimmed (no business
#   here right now — pickup volume is low).
# - `prime`: the tightest "best time to be near it" window(s) — the
#   subset of `peak` when this building type most reliably throws a
#   long trip. Drives the pulsing ring at the flag's pole base on the
#   map. Always a subset of `peak`, so a pulsing flag is also at full
#   brightness. Grounded in building busy-hour patterns: airport
#   arrival banks (5–9am / 4–9pm), hospital discharges peaking ~4pm,
#   hotel morning airport runs (luggage out 7–11am), transit + corporate
#   evening rush (5:30–6:30pm), school pickup (2:30–4pm). Heuristics, a
#   hint for the driver — not measured trip data.
# - `weekday_only`: if true, weekends count as "off" (and never prime)
#   regardless of hour (corporate towers, schools, conventions — none of
#   those generate trips on a Sunday at 3pm even though the clock is
#   "midday"). Federal holidays count the same way — see holiday_calendar.
#
# Hour ranges can wrap past midnight ([23, 5] = 11pm to 5am).
# Anything not in `peak` and not in `off` is "medium" — neither dimmed
# nor highlighted. Hospitals + airports have no off hours: they
# generate trips 24/7.
#
# Every window is a *pickup* window — when someone LEAVES the building
# for a trip (hotel checkout, office end-of-day, hospital discharge),
# never when they arrive. Arrivals (hotel check-in, the morning office /
# school drop-off) are someone else's drop-off, not a pickup here, so
# they are deliberately NOT peak/prime. The date dimension (holidays +
# the school year) lives in holiday_calendar.py and is applied per the
# weekday_only rule above plus per-category seasonal closures.
CATEGORY_DIM_SCHEDULE: Dict[str, Dict[str, Any]] = {
    "airport":         {"peak": [[5, 9], [16, 22]],  "off": [],          "weekday_only": False, "prime": [[6, 9], [16, 21]]},
    "hospital":        {"peak": [[10, 17]],          "off": [],          "weekday_only": False, "prime": [[13, 17]]},
    "hotel_luxury":    {"peak": [[6, 12]],           "off": [[23, 6]],   "weekday_only": False, "prime": [[7, 11]]},
    "transit_hub":     {"peak": [[7, 10], [16, 20]], "off": [[23, 5]],   "weekday_only": False, "prime": [[7, 9], [17, 20]]},
    "corporate":       {"peak": [[16, 20]],          "off": [[20, 7]],   "weekday_only": True,  "prime": [[16, 19]]},
    "private_school":  {"peak": [[7, 9], [14, 16]],  "off": [[19, 6]],   "weekday_only": True,  "prime": [[14, 16]]},
    "private_club":    {"peak": [[12, 14], [18, 22]], "off": [[0, 10]],  "weekday_only": False, "prime": [[19, 22]]},
    "luxury_condo":    {"peak": [[7, 9]],             "off": [[22, 6]],  "weekday_only": False, "prime": [[7, 9]]},
    "luxury_shopping": {"peak": [[11, 19]],           "off": [[20, 10]], "weekday_only": False, "prime": [[14, 18]]},
    "performance":     {"peak": [[19, 23]],           "off": [[2, 12]],  "weekday_only": False, "prime": [[22, 23]]},
    "stadium":         {"peak": [[19, 23]],           "off": [[2, 12]],  "weekday_only": False, "prime": [[22, 23]]},
    "convention":      {"peak": [[9, 17]],            "off": [[19, 7]],  "weekday_only": True,  "prime": [[15, 17]]},
}


def _dim_schedule_for(category: str) -> Dict[str, Any]:
    sched = CATEGORY_DIM_SCHEDULE.get(category)
    if not sched:
        # Unknown category → no dim hints, frontend treats as always-on
        # and never pulses (empty prime).
        return {"peak": [], "off": [], "weekday_only": False, "prime": []}
    return {
        "peak": [list(r) for r in sched["peak"]],
        "off": [list(r) for r in sched["off"]],
        "weekday_only": bool(sched["weekday_only"]),
        "prime": [list(r) for r in sched.get("prime", [])],
    }


def hotspot_runtime_meta(
    dominant_category: str, members: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Time-of-day + rationale fields for a stored hotspot, recomputed at
    read time from its dominant category and member list.

    These are all pure functions of the static category tables above, so
    GET /long_trip_hotspots recomputes them on read instead of persisting
    them: no DB column/migration, the dim + pulse signal works for
    already-stored rows, and editing a schedule takes effect on the next
    request without an admin rebuild.

    Returns: best_hours, dim_schedule (peak/off/weekday_only/prime),
    rationale, category_counts.
    """
    cats = [str(m.get("category", "")) for m in members if isinstance(m, dict)]
    counts, rationale = summarize_categories(cats)
    return {
        "best_hours": _best_hours_for(dominant_category),
        "dim_schedule": _dim_schedule_for(dominant_category),
        "rationale": rationale,
        "category_counts": counts,
    }


def haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R_MI = 3958.7613
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return R_MI * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _cluster_pois(
    pois: List[Tuple[str, float, float, str, float]],
    radius_mi: float = CLUSTER_RADIUS_MI,
) -> List[List[int]]:
    """
    Complete-link agglomerative clustering. Returns groups of POI indices.

    A POI joins an existing cluster only if it's within radius_mi of
    EVERY current member (max-distance constraint). This prevents the
    "chain" failure mode of single-link where Penn → Times Sq → Grand
    Central all merge through intermediate hotels.

    O(N^2) on the POI list — fine for ~70 POIs.
    """
    n = len(pois)
    # Each cluster is a list of POI indices. Start with no clusters.
    clusters: List[List[int]] = []

    # Visit POIs in order. For each, find the best existing cluster
    # it can join (all members within radius), or start a new cluster.
    for i in range(n):
        _, lat_i, lng_i, _, _ = pois[i]
        best_cluster_idx = -1
        best_max_dist = float("inf")
        for cidx, members in enumerate(clusters):
            # Compute the max distance from poi[i] to any current member.
            max_d = 0.0
            ok = True
            for m in members:
                _, lat_m, lng_m, _, _ = pois[m]
                d = haversine_miles(lat_i, lng_i, lat_m, lng_m)
                if d > radius_mi:
                    ok = False
                    break
                if d > max_d:
                    max_d = d
            if ok and max_d < best_max_dist:
                best_max_dist = max_d
                best_cluster_idx = cidx
        if best_cluster_idx >= 0:
            clusters[best_cluster_idx].append(i)
        else:
            clusters.append([i])

    return clusters


def _dominant_category(member_indices: List[int]) -> str:
    """Highest-priority category present in the cluster."""
    cats_in_cluster = {NYC_LONG_TRIP_POIS[i][3] for i in member_indices}
    for c in reversed(_CATEGORY_PRIORITY):
        if c in cats_in_cluster:
            return c
    return "tourist"


def _cluster_label(member_indices: List[int]) -> str:
    """Short human-readable label for the cluster pin."""
    members = [NYC_LONG_TRIP_POIS[i] for i in member_indices]
    if len(members) == 1:
        return members[0][0]
    # Find the highest-weight member as the anchor name.
    members.sort(key=lambda m: -m[4])
    anchor = members[0][0]
    extra = len(members) - 1
    return f"{anchor} +{extra}" if extra > 0 else anchor


def build_long_trip_hotspots() -> List[Dict[str, Any]]:
    """
    Returns the cluster pins as a list of dicts:
      {
        "id": int,
        "lat": float, "lng": float,    # weighted centroid
        "label": str,
        "dominant_category": str,
        "member_count": int,
        "total_weight": float,
        "members": [{"name": str, "category": str, "weight": float}, ...],
      }
    """
    groups = _cluster_pois(NYC_LONG_TRIP_POIS, CLUSTER_RADIUS_MI)
    hotspots: List[Dict[str, Any]] = []
    next_id = 1
    for indices in groups:
        # A hotspot normally requires MIN_MEMBERS_PER_HOTSPOT (3 by default)
        # POIs clustered together. Singletons and isolated pairs are
        # dropped — they're not significant enough to merit an icon.
        # Exception: a 2-member cluster anchored by a hospital still
        # renders. A major hospital is a 24/7 trip generator, so a hospital
        # plus one real neighbor is a legitimate strategic point (e.g. NYU
        # Langone Brooklyn + the ferry terminal on the Sunset Park West
        # waterfront). Lone hospital singletons are still dropped to keep
        # the map uncluttered.
        if len(indices) < MIN_MEMBERS_PER_HOTSPOT and not (
            _cluster_has_hospital(indices) and len(indices) >= 2
        ):
            continue
        # Weighted centroid: sum(lat * weight) / sum(weight), same for lng.
        total_w = 0.0
        lat_w = 0.0
        lng_w = 0.0
        members: List[Dict[str, Any]] = []
        for i in indices:
            name, lat, lng, cat, w = NYC_LONG_TRIP_POIS[i]
            total_w += w
            lat_w += lat * w
            lng_w += lng * w
            # Include each member's lat/lng so the frontend can render a
            # small "building" dot at the actual POI location alongside
            # the cluster's centroid dollar-flag pin. Address and
            # best-hours are also baked in so the popup can show them
            # without a second round-trip per click.
            members.append({
                "name": name, "category": cat, "weight": w,
                "lat": lat, "lng": lng,
                "address": _poi_address(name),
                "best_hours": _best_hours_for(cat),
            })
        if total_w <= 0:
            continue
        members.sort(key=lambda m: -m["weight"])
        # Raw weighted centroid (mathematical middle of the buildings).
        lat_centroid = lat_w / total_w
        lng_centroid = lng_w / total_w
        # Snap the flag's actual stand-spot to the member POI closest to
        # the centroid. This guarantees the pin sits on a real building
        # face the driver can navigate to, not in the middle of an
        # intersection.
        stand_lat, stand_lng = _nearest_member_to_centroid(
            indices, lat_centroid, lng_centroid,
        )
        dom_cat = _dominant_category(indices)
        counts, rationale = _category_summary(indices)
        hotspots.append({
            "id": next_id,
            "lat": round(stand_lat, 6),
            "lng": round(stand_lng, 6),
            "centroid_lat": round(lat_centroid, 6),
            "centroid_lng": round(lng_centroid, 6),
            "label": _cluster_label(indices),
            "dominant_category": dom_cat,
            "member_count": len(indices),
            "total_weight": round(total_w, 3),
            "rationale": rationale,
            "category_counts": counts,
            "best_hours": _best_hours_for(dom_cat),
            "dim_schedule": _dim_schedule_for(dom_cat),
            "members": members,
        })
        next_id += 1
    # Sort by total_weight descending — drivers see the strongest
    # generators first if there's any list view.
    hotspots.sort(key=lambda h: -h["total_weight"])
    return hotspots


def write_long_trip_hotspots(
    db_exec: Callable[..., Any],
) -> Dict[str, Any]:
    """
    Build once, UPSERT into long_trip_hotspots table. Returns a summary.
    """
    import time
    hotspots = build_long_trip_hotspots()
    now_unix = int(time.time())

    # Clear the table first — cluster IDs aren't stable across POI list
    # edits (adding one POI can merge/split clusters), so a full replace
    # is safer than UPSERT-by-id.
    db_exec("DELETE FROM long_trip_hotspots")
    for h in hotspots:
        db_exec(
            """
            INSERT INTO long_trip_hotspots
                (id, lat, lng, label, dominant_category, member_count,
                 total_weight, members_json, generated_at_unix)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(h["id"]), float(h["lat"]), float(h["lng"]),
                str(h["label"]), str(h["dominant_category"]),
                int(h["member_count"]), float(h["total_weight"]),
                json.dumps(h["members"]), int(now_unix),
            ),
        )

    return {
        "hotspots_count": len(hotspots),
        "poi_count": len(NYC_LONG_TRIP_POIS),
        "cluster_radius_mi": CLUSTER_RADIUS_MI,
        "generated_at_unix": now_unix,
    }

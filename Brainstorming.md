# Design
Cloud Function <- NYC Open Data Portal
|
|
!!!Save/Update each record in Firestore
Problem: too expensive
Solutions:
1. Save as single/few JSON documents in Firestore
+ Save operations
- Need to be parsed and saved to the local DB on the client
- Cannot be used for real-time map rendering directly with Firestore

2. Save clusters data in Firestore. E.g.
{
    'zoomLevel':12,
    'clusterId': 'Ab12',
    'boundsNESW': [
        {},{},{},{}
    ],
    'markers':[
        {},{},{}
    ]
}

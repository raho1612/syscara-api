import os
import sys

from main import app

def test_routes():
    print("Testing /api/kosten/fahrzeuge")
    with app.test_client() as client:
        # Test fahrzeuge route
        response = client.get('/api/kosten/fahrzeuge?von=2024-01-01&bis=2024-12-31&art=alle')
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.get_json()
            print(f"Success: {data.get('success')}, Count: {data.get('count')}")
        else:
            print(f"Error Body: {response.text}")

        # Test deckungsbeitrag route
        print("\nTesting /api/kosten/deckungsbeitrag")
        response = client.post('/api/kosten/deckungsbeitrag', json={})
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.get_json()
            print(f"Success: {data.get('success')}")
        else:
            print(f"Error Body: {response.text}")

if __name__ == '__main__':
    test_routes()

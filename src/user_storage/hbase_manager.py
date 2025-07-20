#!/usr/bin/env python3
"""
HBase User Storage Manager
Handles user profiles and interaction history for recommendation system
"""
import happybase
import json
from typing import Dict, List, Optional
from datetime import datetime

class HBaseUserStorage:
    """
    Manages user data storage in HBase for content recommendation system
    """
    
    def __init__(self, host='hbase', port=9090):
        self.connection = happybase.Connection(host=host, port=port)
        self.user_profiles_table = self.connection.table('user_profiles')
        self.user_interactions_table = self.connection.table('user_interactions')
        self.content_catalog_table = self.connection.table('content_catalog')
        print("Connected to HBase")
    
    def store_user_profile(self, user_id: str, profile_data: Dict):
        """Store user profile in HBase"""
        row_key = user_id.encode()
        
        data = {
            b'info:age_group': profile_data.get('age_group', '').encode(),
            b'info:location': profile_data.get('location', '').encode(),
            b'info:subscription_tier': profile_data.get('subscription_tier', '').encode(),
            b'info:preferred_device': profile_data.get('preferred_device', '').encode(),
            b'preferences:genres': json.dumps(profile_data.get('preferred_genres', [])).encode(),
            b'preferences:activity_level': profile_data.get('activity_level', '').encode()
        }
        
        self.user_profiles_table.put(row_key, data)
    
    def store_user_interaction(self, user_id: str, interaction_data: Dict):
        """Store user interaction event in HBase"""
        timestamp = str(int(datetime.now().timestamp() * 1000))
        row_key = f"{user_id}_{timestamp}".encode()
        
        data = {
            b'events:content_id': interaction_data.get('content_id', '').encode(),
            b'events:event_type': interaction_data.get('event_type', '').encode(),
            b'events:session_id': interaction_data.get('session_id', '').encode(),
            b'events:timestamp': interaction_data.get('timestamp', '').encode(),
            b'events:device_type': interaction_data.get('device_type', '').encode(),
        }
        
        # Add optional fields if present
        if interaction_data.get('watch_duration'):
            data[b'events:watch_duration'] = str(interaction_data['watch_duration']).encode()
        if interaction_data.get('rating_score'):
            data[b'ratings:score'] = str(interaction_data['rating_score']).encode()
        if interaction_data.get('search_query'):
            data[b'events:search_query'] = interaction_data['search_query'].encode()
        
        self.user_interactions_table.put(row_key, data)
    
    def get_user_profile(self, user_id: str) -> Optional[Dict]:
        """Retrieve user profile from HBase"""
        row_key = user_id.encode()
        row = self.user_profiles_table.row(row_key)
        
        if not row:
            return None
        
        profile = {
            'user_id': user_id,
            'age_group': row.get(b'info:age_group', b'').decode(),
            'location': row.get(b'info:location', b'').decode(),
            'subscription_tier': row.get(b'info:subscription_tier', b'').decode(),
            'preferred_device': row.get(b'info:preferred_device', b'').decode(),
            'preferred_genres': json.loads(row.get(b'preferences:genres', b'[]').decode()),
            'activity_level': row.get(b'preferences:activity_level', b'').decode()
        }
        return profile
    
    def get_user_interactions(self, user_id: str, limit: int = 100) -> List[Dict]:
        """Get user interaction history"""
        prefix = f"{user_id}_".encode()
        interactions = []
        
        for key, data in self.user_interactions_table.scan(row_prefix=prefix, limit=limit):
            interaction = {
                'content_id': data.get(b'events:content_id', b'').decode(),
                'event_type': data.get(b'events:event_type', b'').decode(),
                'timestamp': data.get(b'events:timestamp', b'').decode(),
                'device_type': data.get(b'events:device_type', b'').decode(),
            }
            
            # Add optional fields
            if b'events:watch_duration' in data:
                interaction['watch_duration'] = int(data[b'events:watch_duration'].decode())
            if b'ratings:score' in data:
                interaction['rating_score'] = float(data[b'ratings:score'].decode())
            if b'events:search_query' in data:
                interaction['search_query'] = data[b'events:search_query'].decode()
            
            interactions.append(interaction)
        
        return interactions
    
    def close(self):
        """Close HBase connection"""
        self.connection.close()

if __name__ == "__main__":
    # Test HBase connection
    storage = HBaseUserStorage()
    
    # Test storing a user profile
    test_profile = {
        'age_group': '26-35',
        'location': 'US',
        'subscription_tier': 'premium',
        'preferred_device': 'mobile',
        'preferred_genres': ['Action', 'Comedy'],
        'activity_level': 'high'
    }
    
    storage.store_user_profile('test_user_001', test_profile)
    print("Stored test user profile")
    
    # Test retrieving user profile
    retrieved = storage.get_user_profile('test_user_001')
    print(f"Retrieved profile: {retrieved}")
    
    storage.close()

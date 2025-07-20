#!/usr/bin/env python3
"""
Enhanced Event Producer for Realistic Streaming Platform Data
Generates multiple interactions per user with realistic viewing patterns
"""
import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from typing import Dict, List, Any
import uuid

class EnhancedStreamingEventProducer:
    """Enhanced producer that generates realistic user behavior patterns"""
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            print(f"Connected to Kafka at {bootstrap_servers}")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            # Fallback for local testing
            self.producer = None
    
    def generate_realistic_content_catalog(self) -> List[Dict]:
        """Generate realistic content with genres and metadata"""
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 'Animation', 'Thriller', 'Adventure']
        content_types = ['movie', 'series', 'documentary', 'short']
        
        catalog = []
        for i in range(500):  # Smaller catalog for higher overlap
            content = {
                'content_id': f'content_{i:04d}',
                'title': f'Content Title {i}',
                'genre': random.choice(genres),
                'content_type': random.choice(content_types),
                'duration': random.randint(30, 180),  # minutes
                'release_year': random.randint(2015, 2025),
                'rating': round(random.uniform(1.0, 5.0), 1)
            }
            catalog.append(content)
        
        return catalog
    
    def generate_user_personas(self, num_users: int = 200) -> List[Dict]:
        """Generate user personas with preferred genres"""
        personas = []
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 'Animation', 'Thriller', 'Adventure']
        
        for i in range(num_users):
            # Each user has 1-3 preferred genres
            preferred_genres = random.sample(genres, random.randint(1, 3))
            activity_level = random.choice(['low', 'medium', 'high'])
            
            persona = {
                'user_id': f'user_{i:05d}',
                'preferred_genres': preferred_genres,
                'activity_level': activity_level,
                'session_duration_preference': random.randint(30, 240),  # minutes
                'discovery_tendency': random.uniform(0.1, 0.9)  # How likely to try new content
            }
            personas.append(persona)
        
        return personas
    
    def simulate_user_session(self, user: Dict, content_catalog: List[Dict]) -> List[Dict]:
        """Simulate a realistic user session with multiple interactions"""
        events = []
        
        # Determine session characteristics
        activity_multiplier = {'low': 0.5, 'medium': 1.0, 'high': 2.0}
        base_interactions = int(random.randint(3, 15) * activity_multiplier[user['activity_level']])
        
        # Filter content by user preferences (80% preferred genres, 20% discovery)
        preferred_content = [c for c in content_catalog if c['genre'] in user['preferred_genres']]
        discovery_content = [c for c in content_catalog if c['genre'] not in user['preferred_genres']]
        
        session_start = datetime.now() - timedelta(hours=random.randint(0, 72))
        
        for i in range(base_interactions):
            # 80% chance to interact with preferred content
            if random.random() < 0.8 and preferred_content:
                content = random.choice(preferred_content)
            else:
                content = random.choice(discovery_content) if discovery_content else random.choice(content_catalog)
            
            # Generate interaction sequence for this content
            content_events = self._generate_content_interaction_sequence(user, content, session_start)
            events.extend(content_events)
            
            # Advance time for next interaction
            session_start += timedelta(minutes=random.randint(1, 30))
        
        return events
    
    def _generate_content_interaction_sequence(self, user: Dict, content: Dict, start_time: datetime) -> List[Dict]:
        """Generate a realistic sequence of interactions for a piece of content"""
        events = []
        current_time = start_time
        
        # Always start with a view
        watch_duration = self._calculate_watch_duration(user, content)
        
        view_event = {
            'event_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'content_id': content['content_id'],
            'event_type': 'view',
            'timestamp': current_time.isoformat(),
            'session_id': f"session_{random.randint(1000, 9999)}",
            'device_type': random.choice(['mobile', 'desktop', 'tv', 'tablet']),
            'watch_duration': watch_duration,
            'content_metadata': {
                'genre': content['genre'],
                'content_type': content['content_type'],
                'duration': content['duration']
            }
        }
        events.append(view_event)
        current_time += timedelta(seconds=30)
        
        # Based on watch duration and user preferences, generate additional interactions
        engagement_score = self._calculate_engagement_score(user, content, watch_duration)
        
        # Click events (navigation within content)
        if watch_duration > 60:  # If watched for more than 1 minute
            num_clicks = random.randint(0, 3)
            for _ in range(num_clicks):
                click_event = {
                    'event_id': str(uuid.uuid4()),
                    'user_id': user['user_id'],
                    'content_id': content['content_id'],
                    'event_type': 'click',
                    'timestamp': current_time.isoformat(),
                    'session_id': view_event['session_id'],
                    'device_type': view_event['device_type'],
                    'click_position': random.randint(1, watch_duration)
                }
                events.append(click_event)
                current_time += timedelta(seconds=random.randint(5, 30))
        
        # Like/dislike based on engagement
        if engagement_score > 0.6:
            like_event = {
                'event_id': str(uuid.uuid4()),
                'user_id': user['user_id'],
                'content_id': content['content_id'],
                'event_type': 'like',
                'timestamp': current_time.isoformat(),
                'session_id': view_event['session_id'],
                'device_type': view_event['device_type']
            }
            events.append(like_event)
            current_time += timedelta(seconds=5)
        
        # Rating for highly engaged content
        if engagement_score > 0.7 and random.random() < 0.3:
            rating = max(1, min(5, int(engagement_score * 5 + random.uniform(-0.5, 0.5))))
            rating_event = {
                'event_id': str(uuid.uuid4()),
                'user_id': user['user_id'],
                'content_id': content['content_id'],
                'event_type': 'rating',
                'timestamp': current_time.isoformat(),
                'session_id': view_event['session_id'],
                'device_type': view_event['device_type'],
                'rating_score': rating
            }
            events.append(rating_event)
        
        # Search events (user looking for similar content)
        if engagement_score > 0.5 and random.random() < 0.2:
            search_event = {
                'event_id': str(uuid.uuid4()),
                'user_id': user['user_id'],
                'content_id': content['content_id'],
                'event_type': 'search',
                'timestamp': current_time.isoformat(),
                'session_id': view_event['session_id'],
                'device_type': view_event['device_type'],
                'search_query': content['genre'].lower()
            }
            events.append(search_event)
        
        return events
    
    def _calculate_watch_duration(self, user: Dict, content: Dict) -> int:
        """Calculate realistic watch duration based on user preferences and content"""
        base_duration = content['duration'] * 60  # Convert to seconds
        
        # Preference bonus
        preference_bonus = 1.0
        if content['genre'] in user['preferred_genres']:
            preference_bonus = random.uniform(1.2, 2.0)
        
        # Activity level impact
        activity_impact = {'low': 0.6, 'medium': 1.0, 'high': 1.4}[user['activity_level']]
        
        # Quality impact (based on content rating)
        quality_impact = min(1.5, content['rating'] / 3.0)
        
        # Calculate final duration with some randomness
        watch_percentage = min(1.0, random.uniform(0.1, 0.9) * preference_bonus * activity_impact * quality_impact)
        return int(base_duration * watch_percentage)
    
    def _calculate_engagement_score(self, user: Dict, content: Dict, watch_duration: int) -> float:
        """Calculate engagement score based on various factors"""
        content_duration = content['duration'] * 60
        
        # Base engagement from watch percentage
        watch_percentage = watch_duration / content_duration
        base_engagement = min(1.0, watch_percentage * 2)  # Watching 50% = max base engagement
        
        # Genre preference bonus
        genre_bonus = 0.3 if content['genre'] in user['preferred_genres'] else 0
        
        # Quality bonus
        quality_bonus = (content['rating'] - 3.0) / 10.0  # Rating above 3 gives bonus
        
        # Activity level impact
        activity_impact = {'low': 0.8, 'medium': 1.0, 'high': 1.2}[user['activity_level']]
        
        final_score = min(1.0, (base_engagement + genre_bonus + quality_bonus) * activity_impact)
        return max(0.0, final_score)
    
    def generate_enhanced_dataset(self, num_events: int = 5000):
        """Generate enhanced dataset with realistic user behavior"""
        print(f"Generating enhanced dataset with {num_events} events...")
        
        # Generate content catalog and user personas
        content_catalog = self.generate_realistic_content_catalog()
        user_personas = self.generate_user_personas()
        
        print(f"Created {len(content_catalog)} content items and {len(user_personas)} user personas")
        
        all_events = []
        
        # Generate sessions for each user
        for user in user_personas:
            # Each user has 1-3 sessions over time
            num_sessions = random.randint(1, 3)
            for session in range(num_sessions):
                session_events = self.simulate_user_session(user, content_catalog)
                all_events.extend(session_events)
        
        # Shuffle events to simulate real-time arrival
        random.shuffle(all_events)
        
        # Limit to requested number of events
        all_events = all_events[:num_events]
        
        print(f"Generated {len(all_events)} total events")
        
        # Send to Kafka
        if self.producer:
            sent_count = 0
            for event in all_events:
                try:
                    future = self.producer.send('user-interactions', value=event, key=event['user_id'])
                    future.get(timeout=10)
                    sent_count += 1
                    
                    if sent_count % 100 == 0:
                        print(f"Sent {sent_count} events...")
                        
                except Exception as e:
                    print(f"Failed to send event: {e}")
            
            print(f"Successfully sent {sent_count} events to Kafka")
            self.producer.flush()
        else:
            print("No Kafka producer available - events generated but not sent")
        
        # Print statistics
        self._print_dataset_statistics(all_events)
        
        return all_events
    
    def _print_dataset_statistics(self, events: List[Dict]):
        """Print statistics about the generated dataset"""
        users = set(event['user_id'] for event in events)
        content_items = set(event['content_id'] for event in events)
        event_types = {}
        
        for event in events:
            event_type = event['event_type']
            event_types[event_type] = event_types.get(event_type, 0) + 1
        
        print("\n=== Dataset Statistics ===")
        print(f"Total events: {len(events)}")
        print(f"Unique users: {len(users)}")
        print(f"Unique content: {len(content_items)}")
        print(f"Avg interactions per user: {len(events)/len(users):.1f}")
        print(f"Matrix density: {(len(events)/(len(users)*len(content_items)))*100:.2f}%")
        print(f"Event type distribution: {event_types}")
    
    def close(self):
        """Close the producer"""
        if self.producer:
            self.producer.close()

if __name__ == "__main__":
    # Test the enhanced producer
    producer = EnhancedStreamingEventProducer()
    events = producer.generate_enhanced_dataset(num_events=5000)
    producer.close()
